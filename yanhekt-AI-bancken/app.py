"""
文件名：app.py
Insight Ingestion + Search API (cleaned)

功能：
  ① /api/precheck：下载前预检，生成 objectId 并判断是否已存在（本地成品/原始 TS）
  ② /api/ingestions：分片上传会话（init/segments/missing/complete/status），后台合并 +（可选）无损转封装
  ③ /api/download/<objectId>：下载本地成品
  ④ /api/search：示例（依赖 es_indexer），按 videoId=objectId 过滤
  ⑤ /api/insights + /api/insights/<id>/status：洞悉管线（可复用/断点续跑）
  ⑥ /api/health：健康检查

状态机（stage）：
  PRECHECK_HIT / PRECHECK_MISS
  UPLOADING(进度) → QUEUED → MERGING(进度) → MERGED → TRANSCODING(进度) → DONE / FAILED

命名：
  objectId = sha1(f"{courseId}|{videoId}|{videoType}|{startedAt}")[:16]
  finals/{objectId}.ts / finals/{objectId}.mp4
"""
from __future__ import annotations

import os
import sys
import json
import time
import uuid
import shutil
import hashlib
import threading
import subprocess
from typing import Dict, Any

from flask import Flask, request, jsonify, send_file,redirect
from flask_cors import CORS

# ===================== Config & Paths =====================

def _load_config_paths():
    """Resolve temp/final dirs from config or env with safe defaults."""
    try:
        import config  # type: ignore
        temp_dir = getattr(config, "TEMP_UPLOAD_DIR", None)
        final_dir = getattr(config, "FINAL_VIDEO_DIR", None)
    except Exception:
        temp_dir = None
        final_dir = None

    temp_dir = temp_dir or os.environ.get("INSIGHT_TEMP_DIR")
    final_dir = final_dir or os.environ.get("INSIGHT_FINAL_DIR")

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
    temp_dir = temp_dir or os.path.join(base, "uploads")
    final_dir = final_dir or os.path.join(base, "finals")

    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(final_dir, exist_ok=True)
    return temp_dir, final_dir

TEMP_UPLOAD_DIR, FINAL_VIDEO_DIR = _load_config_paths()

INSIGHTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "insights"))
os.makedirs(INSIGHTS_DIR, exist_ok=True)

STATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "state_schema.json"))
_state_registry_lock = threading.Lock()

# Optional integrations
try:
    import es_indexer  # noqa: F401
except Exception as e:  # pragma: no cover
    es_indexer = None
    print(f"[WARN] es_indexer 导入失败：{e}", file=sys.stderr)

def _index_for_object(object_id: str) -> str:
    safe = "".join(ch for ch in object_id.lower() if ch.isalnum() or ch in ("-", "_"))
    return f"yanhe-video-{safe}"

try:
    import oss_uploader  # noqa: F401
except Exception:
    oss_uploader = None

try:
    import tingwu_analyzer  # noqa: F401
except Exception:
    tingwu_analyzer = None

try:
    import json_transformer  # noqa: F401
except Exception:
    json_transformer = None

# ===================== Utilities =====================

def _make_object_id(course_id: str, video_id: int, video_type: str, started_at: str) -> str:
    key = f"{course_id}|{video_id}|{video_type}|{started_at}".encode("utf-8")
    return hashlib.sha1(key).hexdigest()[:16]

def _uploaddir(upload_id: str) -> str: return os.path.join(TEMP_UPLOAD_DIR, upload_id)

def _partsdir(upload_id: str) -> str: return os.path.join(_uploaddir(upload_id), "parts")

def _metafile(upload_id: str) -> str: return os.path.join(_uploaddir(upload_id), "meta.json")

def _statefile(upload_id: str) -> str: return os.path.join(_uploaddir(upload_id), "state.json")

def _final_ts_by_object(object_id: str) -> str: return os.path.join(FINAL_VIDEO_DIR, f"{object_id}.ts")

def _final_mp4_by_object(object_id: str) -> str: return os.path.join(FINAL_VIDEO_DIR, f"{object_id}.mp4")

def _insight_statefile(object_id: str) -> str: return os.path.join(INSIGHTS_DIR, f"{object_id}.json")

def _save_json(path: str, obj: Dict[str, Any]) -> None:
    """Atomic JSON save (Windows-friendly)."""
    tmp = path + ".tmp"
    data = json.dumps(obj, ensure_ascii=False)
    for i in range(12):
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.02 * (i + 1))
    os.replace(tmp, path)

def _load_json(path: str, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

# ===================== Global State Registry =====================

def _state_load_all() -> dict:
    if not os.path.exists(STATE_PATH):
        return {"$version": 1}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"$version": 1}

def _state_save_all(d: dict) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, STATE_PATH)

def _state_get(object_id: str) -> dict | None:
    with _state_registry_lock:
        all_ = _state_load_all()
        return all_.get(object_id)

def _state_upsert(object_id: str, patch: dict) -> dict:
    """浅合并 + dict 字段的深一层合并；写回 state_schema.json"""
    with _state_registry_lock:
        all_ = _state_load_all()
        cur = all_.get(object_id, {
            "objectId": object_id, "createdAt": time.time(), "attempts": 0,
            "stage": "CHECK", "progress": 0, "message": "", "meta": {},
            "oss": {"remoteKey": None, "signedUrl": None, "uploaded": False},
            "tingwu": {"taskId": None, "status": None, "result": {}},
            # +++ 修改这里：新增 "srtPath": None +++
            "results": {"asrPath": None, "pptPath": None, "pdfPath": None, "srtPath": None},
            "error": None
        })
        cur.setdefault("once", {})
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(cur.get(k), dict):
                cur[k].update(v)
            else:
                cur[k] = v
        cur["updatedAt"] = time.time()
        all_[object_id] = cur
        _state_save_all(all_)
        return cur

# ===================== Insights Pipeline =====================

def _post_fetch_pipeline(object_id: str) -> None:
    """在已拿到 tingwu.result 的前提下：下载→解析→入 ES→DONE（全部幂等）。"""
    try:
        st = _state_get(object_id) or {}
        once = st.get("once") or {}
        result_urls = ((st.get("tingwu") or {}).get("result") or {})
        if not result_urls:
            return  # 还没拿到 URL，就不继续

        # 1) 下载（幂等）
        results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "analysis_results"))
        os.makedirs(results_dir, exist_ok=True)
        asr_url = result_urls.get("Transcription")
        ppt_url = result_urls.get("PptExtraction")
        asr_path = os.path.join(results_dir, f"{object_id}_ASR_Result.json")
        ppt_path = os.path.join(results_dir, f"{object_id}_PPT_Result.json")
        pdf_path = os.path.join(results_dir, f"{object_id}_PPT_Result.pdf")

        # +++ 新增：定义 SRT 路径 +++
        srt_path = os.path.join(results_dir, f"{object_id}_Subtitles.srt")
        pdf_url_to_save = None
        if not once.get("dl_results"):
            if asr_url and hasattr(tingwu_analyzer, "download_result_json") and not os.path.exists(asr_path):
                tingwu_analyzer.download_result_json(asr_url, asr_path)

            # --- 修复：下载 PPT JSON 并提取 PDF URL (不下载PDF) ---
            if ppt_url and hasattr(tingwu_analyzer, "download_result_json"):
                # 步骤 1: 确保 PPT JSON 文件被下载 (幂等)
                if not os.path.exists(ppt_path):
                    print(f"[Insight] (post-fetch) 正在下载 PPT JSON: {ppt_path}")
                    tingwu_analyzer.download_result_json(ppt_url, ppt_path)

                # 步骤 2: 从 *本地* 加载刚下载的 PPT JSON 文件
                ppt_json = _load_json(ppt_path)

                # 步骤 3: 提取 PDF URL
                if ppt_json:
                    pdf_url_to_save = (ppt_json.get("PptExtraction", {}) or {}).get("PdfPath")
                    if not pdf_url_to_save:
                        print(f"[WARN] (post-fetch) 未能在 {ppt_path} 中找到 PdfPath")
            # --- 修复结束 ---

            # +++ 新增：调用 SRT 生成 +++
            if json_transformer and hasattr(json_transformer, "generate_srt_file"):
                if os.path.exists(asr_path):
                    try:
                        print(f"[Insight] (post-fetch) 正在生成 SRT 字幕文件: {srt_path}")
                        json_transformer.generate_srt_file(asr_path, srt_path)
                    except Exception as e:
                        print(f"[WARN] (post-fetch) SRT 字幕文件生成失败: {e}", file=sys.stderr)

            _state_upsert(object_id, {"once": {"dl_results": time.time()}})  # 标记下载已完成

        else:
            # 如果之前已运行过，尝试从 state 或本地文件恢复 URL
            st = _state_get(object_id) or {}
            pdf_url_to_save = (st.get("results") or {}).get("pdfPath")
            if not pdf_url_to_save and os.path.exists(ppt_path):
                ppt_json = _load_json(ppt_path)
                if ppt_json:
                    pdf_url_to_save = (ppt_json.get("PptExtraction", {}) or {}).get("PdfPath")

        _insight_save(object_id, "DOWNLOAD_RESULTS", 0.78, "保存结果到本地", {
            "results": {
                "asrPath": asr_path if os.path.exists(asr_path) else None,
                "pptPath": ppt_path if os.path.exists(ppt_path) else None,
                "pdfPath": pdf_url_to_save,  # <--- 关键修改：保存 URL 字符串
                "srtPath": srt_path if os.path.exists(srt_path) else None,
            }
        })
        # _state_upsert(object_id, {"once": {"dl_results": time.time()}}) # <-- 移动到 _insight_save 之前

        # 2) ES 入索引（幂等）
        st = _state_get(object_id) or {}
        once = st.get("once") or {}
        if es_indexer and json_transformer and not once.get("es_index"):
            _insight_save(object_id, "ES_INDEX", 0.80, "构建知识卡片/入索引")
            cards = []
            if os.path.exists(asr_path):
                cards += json_transformer.parse_asr_json_to_cards(asr_path, object_id)
            if os.path.exists(ppt_path):
                cards += json_transformer.parse_ppt_json_to_cards(ppt_path, object_id)
            if cards:
                index_name = _index_for_object(object_id)

                if hasattr(es_indexer, "create_index_if_not_exists"):
                    es_indexer.create_index_if_not_exists(index_name)

                if hasattr(es_indexer, "bulk_index_cards"):
                    es_indexer.bulk_index_cards(cards, index_name)
                elif hasattr(es_indexer, "bulk_index_cards_with_index"):
                    es_indexer.bulk_index_cards_with_index(cards, index_name)

            _state_upsert(object_id, {"once": {"es_index": time.time()}})
            _insight_save(object_id, "ES_INDEX", 0.90, "入索引完成")

        # 3) 完成
        _insight_save(object_id, "DONE", 1.00, "洞悉完成")
    except Exception as e:
        _insight_save(object_id, "FAILED", 1.00, f"{e}", {"error": str(e)})

def _insight_save(object_id: str, stage: str, progress: float = 0.0, message: str = "", extra: Dict[str, Any] | None = None):
    snap = {"stage": stage, "progress": max(0.0, min(1.0, float(progress))), "message": message, "ts": time.time()}
    if extra:
        snap.update(extra)
    _save_json(_insight_statefile(object_id), snap)

    patch = {"stage": stage, "progress": progress, "message": message}
    if extra:
        if "tingwu" in extra: patch.setdefault("tingwu", {}).update(extra["tingwu"])
        if "oss" in extra: patch.setdefault("oss", {}).update(extra["oss"])
        if "results" in extra: patch.setdefault("results", {}).update(extra["results"])
        if "taskId" in extra: patch.setdefault("tingwu", {})["taskId"] = extra["taskId"]
        for k, v in extra.items():
            if k not in {"tingwu", "oss", "results", "taskId"}:
                patch[k] = v
    _state_upsert(object_id, patch)

def _once_done(object_id: str, step: str) -> bool:
    st = _state_get(object_id) or {}
    return bool((st.get("once") or {}).get(step))

def _once_mark(object_id: str, step: str) -> None:
    _state_upsert(object_id, {"once": {step: time.time()}})
def _index_for_object(object_id: str) -> str:
    safe = "".join(ch for ch in object_id.lower() if ch.isalnum() or ch in ("-", "_"))
    return f"yanhe-video-{safe}"


def _insight_worker(object_id: str):
    try:
        st_prev = _state_get(object_id) or {}
        prev_task_id = ((st_prev.get("tingwu") or {}).get("taskId"))
        prev_result = ((st_prev.get("tingwu") or {}).get("result") or {})
        prev_stage = st_prev.get("stage")

        _insight_save(object_id, "CHECK", 0.0, "开始洞悉")

        # A) 继续轮询既有任务
        if prev_task_id and prev_stage in {"AI_SUBMIT", "AI_POLL", "DOWNLOAD_RESULTS"}:
            _insight_save(object_id, "AI_POLL", 0.3, "继续轮询既有任务", {"tingwu": {"taskId": prev_task_id}})
            while True:
                data = tingwu_analyzer.get_task_status(prev_task_id) if tingwu_analyzer else None
                status = (data or {}).get("TaskStatus")
                if status == "COMPLETED":
                    _insight_save(object_id, "AI_POLL", 0.72, "听悟完成")
                    break
                if status == "FAILED":
                    raise RuntimeError(f"通义听悟失败：{(data or {}).get('ErrorMessage', '')}")
                _insight_save(object_id, "AI_POLL", 0.32, f"听悟状态：{status or 'ONGOING'}")
                time.sleep(2)

        # B) 直接下载结果（若已获取 URL）
        result_urls = None
        if prev_result and prev_stage in {"AI_POLL", "DOWNLOAD_RESULTS", "FAILED"}:
            _insight_save(object_id, "DOWNLOAD_RESULTS", 0.75, "下载结果JSON", {"tingwu": {"result": prev_result}})
            result_urls = prev_result

        # 0) 选择本地文件
        mp4 = _final_mp4_by_object(object_id)
        ts = _final_ts_by_object(object_id)
        local_path = mp4 if os.path.exists(mp4) else ts if os.path.exists(ts) else None
        if not local_path:
            raise RuntimeError("本地成品不存在")
        remote_key = f"insights/{object_id}/{os.path.basename(local_path)}"

        # 1) 上传 OSS（幂等）
        if not _once_done(object_id, "oss_upload"):
            _insight_save(object_id, "OSS_UPLOAD", 0.10, "上传到 OSS")
            if not oss_uploader:
                raise RuntimeError("oss_uploader 模块未加载")

            # --- V2 修复：稳健的重试逻辑 (V2) ---
            max_retries = 3
            upload_ok = False
            last_error = None

            for attempt in range(1, max_retries + 1):
                try:
                    _insight_save(object_id, "OSS_UPLOAD", 0.10 + (attempt * 0.01),
                                  f"尝试上传 (第 {attempt}/{max_retries} 次)")

                    # 1. 执行上传
                    ok = oss_uploader.upload_file_with_progress(local_path, remote_key)

                    # 2. 严格检查返回值 (V2 - 检查字典)
                    #    oss_uploader 在成功时会返回一个字典 {'ok': True, ...}
                    is_success = False
                    if isinstance(ok, dict) and ok.get('ok') is True:
                        is_success = True

                    if not is_success:
                        # 如果返回值不是 {'ok': True, ...}，则视为失败
                        raise RuntimeError(f"上传失败 (返回值: {ok})。oss_uploader 模块可能已打印内部错误。")

                    # 运行到这里，is_success 必定为 True
                    upload_ok = True
                    print(f"[Insight] 上传尝试 {attempt}/{max_retries} 成功 (返回值: {ok})。")
                    break  # 成功，跳出重试循环

                except Exception as e:
                    last_error = e
                    print(f"[Insight] 上传尝试 {attempt}/{max_retries} 失败: {e}", file=sys.stderr)

                    # 如果是 50x 瞬时错误，等待重试
                    if "502" in str(e) or "503" in str(e) or "504" in str(e):
                        if attempt < max_retries:
                            time.sleep(attempt * 2)  # 等待 2s, 4s
                    else:
                        if attempt < max_retries:
                            time.sleep(1)

                            # 循环结束，检查最终结果
            if not upload_ok:
                raise RuntimeError(f"上传 OSS 失败 (重试 {max_retries} 次后): {last_error or 'N/A'}")

            # --- 重试逻辑结束 ---

            _insight_save(object_id, "OSS_UPLOAD", 0.20, "上传完成",
                          {"oss": {"remoteKey": remote_key, "uploaded": True}})
            _once_mark(object_id, "oss_upload")
        else:
            _insight_save(object_id, "OSS_UPLOAD", 0.20, "跳过上传（已完成）",
                          {"oss": {"remoteKey": remote_key, "uploaded": True}})

        # 2) 提交/轮询 & 获取结果URL（与下载分开去重）
        if not (tingwu_analyzer and oss_uploader):
            raise RuntimeError("缺少 tingwu_analyzer 或 oss_uploader 模块")

        # 2.1 创建任务（去重）
        if not prev_task_id and not _once_done(object_id, "ai_submit"):
            signed_url = oss_uploader.get_internal_signed_url_for_tingwu(remote_key)
            if not signed_url:
                raise RuntimeError("获取内网签名URL失败")
            _insight_save(object_id, "AI_SUBMIT", 0.25, "提交通义听悟任务", {"oss": {"signedUrl": signed_url}})
            task_id = tingwu_analyzer.submit_transcription_task(signed_url)
            if not task_id:
                raise RuntimeError("提交听悟任务失败")
            _insight_save(object_id, "AI_POLL", 0.30, "解析中...", {"tingwu": {"taskId": task_id}})
            _once_mark(object_id, "ai_submit")
            prev_task_id = task_id

        # 2.2 轮询直到完成（若尚未获取结果URL）。如果 ai_fetch 已完成则跳过轮询。
        if not _once_done(object_id, "ai_fetch"):
            if not prev_task_id:
                raise RuntimeError("缺少 taskId，无法轮询")
            start = time.time()
            while True:
                data = tingwu_analyzer.get_task_status(prev_task_id)
                status = (data or {}).get("TaskStatus")
                if status == "COMPLETED":
                    _insight_save(object_id, "AI_POLL", 0.72, "听悟完成")
                    result_urls = (data.get("Result") or {})
                    _insight_save(object_id, "DOWNLOAD_RESULTS", 0.75, "记录结果URL",
                                  {"tingwu": {"result": result_urls}})
                    _once_mark(object_id, "ai_fetch")
                    break
                if status == "FAILED":
                    raise RuntimeError(f"通义听悟失败：{(data or {}).get('ErrorMessage', '')}")
                prog = min(0.7, 0.3 + (time.time() - start) / 120.0)
                _insight_save(object_id, "AI_POLL", prog, f"听悟状态：{status or 'ONGOING'}")
                time.sleep(2)
        else:
            # 已经获取过结果URL
            result_urls = (st_prev.get("tingwu") or {}).get("result") or {}

        # 3) 下载结果到本地（幂等）
        # 3) 下载结果到本地（幂等）
        results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "analysis_results"))
        os.makedirs(results_dir, exist_ok=True)

        asr_url = (result_urls or {}).get("Transcription")
        ppt_url = (result_urls or {}).get("PptExtraction")

        asr_path = os.path.join(results_dir, f"{object_id}_ASR_Result.json")
        ppt_path = os.path.join(results_dir, f"{object_id}_PPT_Result.json")
        # pdf_path 已移除，我们只保存 URL
        srt_path = os.path.join(results_dir, f"{object_id}_Subtitles.srt")

        pdf_url_to_save = None  # <--- 用于存储在线 URL

        if not _once_done(object_id, "dl_results"):
            if asr_url and hasattr(tingwu_analyzer, "download_result_json") and not os.path.exists(asr_path):
                tingwu_analyzer.download_result_json(asr_url, asr_path)

            # --- 修复：下载 PPT JSON 并提取 PDF URL (不下载PDF) ---
            if ppt_url and hasattr(tingwu_analyzer, "download_result_json"):
                # 步骤 1: 确保 PPT JSON 文件被下载 (幂等)
                if not os.path.exists(ppt_path):
                    print(f"[Insight] 正在下载 PPT JSON: {ppt_path}")
                    tingwu_analyzer.download_result_json(ppt_url, ppt_path)
                # 步骤 2: 从 *本地* 加载刚下载的 PPT JSON 文件
                ppt_json = _load_json(ppt_path)

                # 步骤 3: 提取 PDF URL (如果存在)
                if ppt_json:
                    pdf_url_to_save = (ppt_json.get("PptExtraction", {}) or {}).get("PdfPath")
                    if not pdf_url_to_save:
                        print(f"[WARN] (insight) 未能在 {ppt_path} 中找到 PdfPath")
            # --- 修复结束 ---

            # +++ 新增：调用 SRT 生成 +++
            if json_transformer and hasattr(json_transformer, "generate_srt_file"):
                if os.path.exists(asr_path):
                    try:
                        print(f"[Insight] 正在生成 SRT 字幕文件: {srt_path}")
                        json_transformer.generate_srt_file(asr_path, srt_path)
                    except Exception as e:
                        print(f"[WARN] SRT 字幕文件生成失败: {e}", file=sys.stderr)

            _once_mark(object_id, "dl_results")

        else:
            # 如果之前已运行过，尝试从 state 或本地文件恢复 URL
            st_prev = _state_get(object_id) or {}
            pdf_url_to_save = (st_prev.get("results") or {}).get("pdfPath")
            if not pdf_url_to_save and os.path.exists(ppt_path):
                ppt_json = _load_json(ppt_path)
                if ppt_json:
                    pdf_url_to_save = (ppt_json.get("PptExtraction", {}) or {}).get("PdfPath")
        _insight_save(object_id, "DOWNLOAD_RESULTS", 0.78, "保存结果到本地", {
            "results": {
                "asrPath": asr_path if os.path.exists(asr_path) else None,
                "pptPath": ppt_path if os.path.exists(ppt_path) else None,
                "pdfPath": pdf_url_to_save,  # <--- 关键修改：保存 URL 字符串
                "srtPath": srt_path if os.path.exists(srt_path) else None,
            }
        })

        # 4) 解析并入 ES（幂等）
        if not _once_done(object_id, "es_index"):
            _insight_save(object_id, "ES_INDEX", 0.80, "构建知识卡片/入索引")
            try:
                if json_transformer and es_indexer:
                    cards: list[dict] = []
                    if os.path.exists(asr_path):
                        cards += json_transformer.parse_asr_json_to_cards(asr_path, object_id)
                    if os.path.exists(ppt_path):
                        cards += json_transformer.parse_ppt_json_to_cards(ppt_path, object_id)

                    if cards:
                        index_name = _index_for_object(object_id)
                        print(f"[ES] index_name={index_name} cards={len(cards)}")  # 关键日志

                        # 创建索引（按 es_indexer 能力分流）
                        if hasattr(es_indexer, "create_index_if_not_exists"):
                            es_indexer.create_index_if_not_exists(index_name)
                        elif hasattr(es_indexer, "ensure_index"):
                            es_indexer.ensure_index(index_name)

                        # 批量写入（优先使用带 index 的方法）
                        if hasattr(es_indexer, "bulk_index_cards"):
                            try:
                                es_indexer.bulk_index_cards(cards, index_name)
                            except TypeError:
                                #  bulk_index_cards 可能没有 index_name 参数 → 兜底到带 index 的变体
                                if hasattr(es_indexer, "bulk_index_cards_with_index"):
                                    es_indexer.bulk_index_cards_with_index(cards, index_name)
                                else:
                                    raise
                        elif hasattr(es_indexer, "bulk_index_cards_with_index"):
                            es_indexer.bulk_index_cards_with_index(cards, index_name)
                        else:
                            raise RuntimeError("es_indexer 缺少可用的 bulk 写入方法")
                # 只有成功后才标记 once
                _once_mark(object_id, "es_index")
                _insight_save(object_id, "ES_INDEX", 0.90, "入索引完成")
            except Exception as e:
                # 不要提前 _once_mark，失败要可重试
                _insight_save(object_id, "FAILED", 1.0, f"ES 索引失败：{e}", {"error": str(e)})
                raise

        # 5) 清理 OSS（幂等）
        if not _once_done(object_id, "oss_clean"):
            _insight_save(object_id, "OSS_CLEAN", 0.95, "清理 OSS")
            if oss_uploader:
                try:
                    oss_uploader.delete_video(remote_key)
                except Exception:
                    pass
            _once_mark(object_id, "oss_clean")

        _insight_save(object_id, "DONE", 1.00, "洞悉完成")

    except Exception as e:
        _insight_save(object_id, "FAILED", 1.00, f"{e}", {"error": str(e)})

# ===================== Upload Merge/Transcode =====================


_state_locks_guard = threading.Lock()
_state_locks: Dict[str, threading.Lock] = {}

def _get_state_lock(upload_id: str) -> threading.Lock:
    with _state_locks_guard:
        lock = _state_locks.get(upload_id)
        if lock is None:
            lock = threading.Lock()
            _state_locks[upload_id] = lock
        return lock

def _find_ffmpeg():
    p = os.environ.get("INSIGHT_FFMPEG")
    if p and os.path.exists(p):
        return p
    try:
        import config  # type: ignore
        p = getattr(config, "FFMPEG_PATH", None)
        if p and os.path.exists(p):
            return p
    except Exception:
        pass
    try:
        import imageio_ffmpeg  # type: ignore
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        pass
    return shutil.which("ffmpeg")

def _merge_and_transcode_worker(upload_id: str) -> None:
    lock = _get_state_lock(upload_id)
    try:
        # MERGING
        state = _load_json(_statefile(upload_id), {}) or {}
        state.update({"stage": "MERGING", "progress": 0})
        with lock: _save_json(_statefile(upload_id), state)

        partsdir = _partsdir(upload_id)
        if not os.path.isdir(partsdir):
            raise RuntimeError("parts dir not found")
        partnames = sorted([n for n in os.listdir(partsdir) if n.startswith("part_")])
        if not partnames:
            raise RuntimeError("no parts found")

        meta = _load_json(_metafile(upload_id), {}) or {}
        object_id = meta.get("objectId")
        if not object_id:
            raise RuntimeError("objectId missing in meta")

        ts_out  = _final_ts_by_object(object_id)
        mp4_out = _final_mp4_by_object(object_id)

        with open(ts_out, "wb") as out:
            for i, name in enumerate(partnames, start=1):
                with open(os.path.join(partsdir, name), "rb") as p:
                    shutil.copyfileobj(p, out, length=1024 * 1024)
                state.update({"stage": "MERGING", "progress": i / len(partnames)})
                with lock: _save_json(_statefile(upload_id), state)

        state.update({"stage": "MERGED", "progress": 1})
        with lock: _save_json(_statefile(upload_id), state)

        # 是否转封装
        if not bool(meta.get("autoTranscode", True)):
            state.update({
                "stage": "DONE", "progress": 1,
                "downloadUrl": f"/api/download/{object_id}?raw=ts",
            })
            with lock: _save_json(_statefile(upload_id), state)
            shutil.rmtree(_uploaddir(upload_id), ignore_errors=True)
            return

        # TRANSCODING
        state.update({"stage": "TRANSCODING", "progress": 0})
        with lock: _save_json(_statefile(upload_id), state)

        ff = _find_ffmpeg()
        if not ff:
            state.update({
                "stage": "DONE", "progress": 1,
                "downloadUrl": f"/api/download/{object_id}?raw=ts",
                "message": "未找到 ffmpeg，已返回 TS 文件（可稍后再转封装）"
            })
            with lock: _save_json(_statefile(upload_id), state)
            shutil.rmtree(_uploaddir(upload_id), ignore_errors=True)
            return

        subprocess.run([ff, "-y", "-loglevel", "error", "-i", ts_out, "-c", "copy", mp4_out], check=True)

        state.update({"stage": "DONE", "progress": 1, "downloadUrl": f"/api/download/{object_id}"})
        with lock: _save_json(_statefile(upload_id), state)

        shutil.rmtree(_uploaddir(upload_id), ignore_errors=True)

    except Exception as e:  # pragma: no cover
        state = _load_json(_statefile(upload_id), {}) or {}
        state.update({"stage": "FAILED", "message": str(e)})
        with lock: _save_json(_statefile(upload_id), state)

# ===================== Flask App =====================

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# -------- Insights APIs --------
@app.route("/api/insights/<object_id>/resume", methods=["POST"])
def insights_resume(object_id: str):
    """续跑：依赖 once 幂等，从缺的步骤继续跑（例如 fetch→下载→ES）。"""
    threading.Thread(target=_insight_worker, args=(object_id,), daemon=True).start()
    return jsonify({"ok": True})
@app.route("/api/insights", methods=["POST", "OPTIONS"])
def insights_create():
    if request.method == "OPTIONS":
        return ("", 200)
    body = request.get_json(force=True, silent=True) or {}
    object_id = str(body.get("objectId") or "").strip()
    if not object_id or len(object_id) != 16:
        return jsonify({"error": "invalid objectId"}), 400

    cur = _state_get(object_id)
    RUNNING_STAGES = {"OSS_UPLOAD", "AI_SUBMIT", "AI_POLL", "DOWNLOAD_RESULTS", "ES_INDEX", "OSS_CLEAN"}
    if cur and cur.get("stage") in RUNNING_STAGES:
        # 任务已在 OSS_UPLOAD 或 AI_POLL 等阶段，直接返回状态
        return jsonify({"ok": True, "stage": cur.get("stage"), "progress": cur.get("progress", 0)})
        # --- 修复结束 ---

        # 如果 stage 是 "CHECK", "DONE", "FAILED", "UNKNOWN" 或 None，则允许重新启动
    base = {"stage": "CHECK", "progress": 0, "message": "开始洞悉"}
    if cur: base["attempts"] = cur.get("attempts", 0) + 1
    _state_upsert(object_id, base)
    threading.Thread(target=_insight_worker, args=(object_id,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/insights/<object_id>/status", methods=["GET", "OPTIONS"])
def insights_status(object_id: str):
    if request.method == "OPTIONS":
        return ("", 200)

    # 如果已经拿到 URL 且还没下载/入索引，则触发一次后台后处理（幂等，不会重复下载）
    st0 = _state_get(object_id) or {}
    once0 = (st0.get("once") or {})
    has_urls = bool(((st0.get("tingwu") or {}).get("result") or {}))
    if has_urls and not once0.get("dl_results"):
        # 防止频繁触发：打一个轻量自旋标记
        _state_upsert(object_id, {"once": {"_kick_dl": time.time()}})
        threading.Thread(target=_post_fetch_pipeline, args=(object_id,), daemon=True).start()

    st = _state_get(object_id) or {"objectId": object_id, "stage": "UNKNOWN", "progress": 0}
    return jsonify({
        "objectId": st.get("objectId"),
        "stage":    st.get("stage"),
        "progress": st.get("progress", 0),
        "message":  st.get("message", ""),
        "attempts": st.get("attempts", 0),
        "createdAt": st.get("createdAt"),
        "updatedAt": st.get("updatedAt")
    })


# =======================================================
# 3. 新增 API：PPT 导出 (重定向到 PDF URL)
# =======================================================
@app.route("/api/insights/<object_id>/ppt", methods=["GET"])
def get_ppt_pdf(object_id: str):
    """
    获取 PPT 对应的 PDF 在线 URL。
    从 state_schema.json 中读取 'results.pdfPath' 字段，
    并重定向到该 URL。
    """
    try:
        state = _state_get(object_id)
        pdf_url = None

        if state:
            pdf_url = state.get("results", {}).get("pdfPath")

        if not pdf_url:
            # 兜底：如果 state 里没有（可能是旧任务），尝试从本地 analysis_results 加载
            # 这与 _insight_worker 和 _post_fetch_pipeline 中的 'else' 逻辑一致
            results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "analysis_results"))
            ppt_path = os.path.join(results_dir, f"{object_id}_PPT_Result.json")

            if os.path.exists(ppt_path):
                # _load_json 是您 app.py 中的辅助函数
                ppt_json = _load_json(ppt_path)
                if ppt_json:
                    pdf_url = (ppt_json.get("PptExtraction", {}) or {}).get("PdfPath")

        if not pdf_url:
            # 如果 state 和本地文件里都找不到，则返回 404
            return jsonify({"error": "PDF URL not found for this objectId. (Is the insight 'DONE'?)."}), 404

        # 成功：重定向浏览器到在线 PDF URL
        return redirect(pdf_url)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------- Result refresh --------

def _ensure_result_urls(object_id: str) -> Dict[str, Any]:
    """若状态里已有 result 直接返回；否则查询一次任务状态（非阻塞轮询），
    完成则写入 result 与 once.ai_fetch，并把 stage 推到 DOWNLOAD_RESULTS。
    """
    st_prev = _state_get(object_id) or {}
    res = ((st_prev.get("tingwu") or {}).get("result") or {})
    if res:
        return res
    task_id = ((st_prev.get("tingwu") or {}).get("taskId"))
    if not task_id or not tingwu_analyzer:
        return {}
    try:
        data = tingwu_analyzer.get_task_status(task_id)
        status = (data or {}).get("TaskStatus")
        if status == "COMPLETED":
            result_urls = (data.get("Result") or {})
            _insight_save(object_id, "DOWNLOAD_RESULTS", 0.75, "记录结果URL", {"tingwu": {"result": result_urls}})
            _once_mark(object_id, "ai_fetch")
            return result_urls
        return {}
    except Exception:
        return {}

# -------- Precheck --------
@app.route("/api/precheck", methods=["POST"])
def precheck():
    body = request.get_json(force=True, silent=True) or {}
    try:
        course_id  = str(body.get("courseId", ""))
        video_id   = int(body.get("videoId"))
        video_type = (body.get("videoType") or "vga")
        started_at = str(body.get("startedAt", ""))

        object_id = _make_object_id(course_id, video_id, video_type, started_at)
        mp4_path = _final_mp4_by_object(object_id)
        ts_path  = _final_ts_by_object(object_id)

        if os.path.exists(mp4_path) or os.path.exists(ts_path):
            return jsonify({
                "objectId": object_id,
                "exists": True,
                "stage": "PRECHECK_HIT",
                "downloadUrl": f"/api/download/{object_id}",
                "rawUrl": f"/api/download/{object_id}?raw=ts"
            })

        return jsonify({"objectId": object_id, "exists": False, "stage": "PRECHECK_MISS"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# -------- Upload Flow --------
@app.route("/api/ingestions", methods=["POST"])
def init_ingestion():
    body = request.get_json(force=True, silent=True) or {}

    # ++++++++++ 在这里添加日志 ++++++++++
    print("="*50)
    print("[Ingestion] 收到来自前端的完整 body:", body)
    # +++++++++++++++++++++++++++++++++++++

    course_id  = str(body.get("courseId"))
    video_id   = int(body.get("videoId", 0))
    video_type = (body.get("videoType") or "vga")
    started_at = str(body.get("startedAt"))
    session_id = int(body.get("sessionId", 0))

    # ++++++++++ 在这里添加更精确的日志 ++++++++++
    print(f"[Ingestion] 准备用于哈希 (objectId) 的数据:")
    print(f"  - course_id:  {course_id}")
    print(f"  - video_id:   {video_id}")
    print(f"  - video_type: {video_type}")
    print(f"  - started_at: {started_at}")
    print("="*50)
    # ++++++++++++++++++++++++++++++++++++++++

    object_id  = _make_object_id(course_id, video_id, video_type, started_at)

    mp4_path = _final_mp4_by_object(object_id)
    ts_path  = _final_ts_by_object(object_id)
    if os.path.exists(mp4_path) or os.path.exists(ts_path):
        _state_upsert(object_id, {"meta": {
            "courseId": course_id,
            "courseName": body.get("courseName"),
            "courseTitle": body.get("courseTitle"),
            "videoType": video_type,
            "videoId": video_id,
            "sessionId": session_id,  # <--- 写入 sessionId
            "startedAt": started_at,
            "originalFilename": body.get("originalFilename") or f"{body.get('courseTitle', 'video')}.ts",
        }})
        return jsonify({
            "objectId": object_id,
            "exists": True,
            "downloadUrl": f"/api/download/{object_id}",
            "rawUrl": f"/api/download/{object_id}?raw=ts"
        })

    upload_id = uuid.uuid4().hex
    os.makedirs(_partsdir(upload_id), exist_ok=True)
    meta = {
        "uploadId": upload_id,
        "objectId": object_id,
        "courseId": course_id,
        "courseName": body.get("courseName"),
        "courseTitle": body.get("courseTitle"),
        "videoType": video_type,
        "videoId": video_id,
        "sessionId": session_id,
        "startedAt": started_at,
        "total": int(body.get("total", 0)),
        "autoTranscode": bool(body.get("autoTranscode", True)),
        "originalFilename": body.get("originalFilename") or f"{body.get('courseTitle','video')}.ts",
        "createdAt": time.time(),
    }
    _save_json(_metafile(upload_id), meta)
    _state_upsert(object_id, {"meta": {
        "courseId": course_id,
        "courseName": body.get("courseName"),
        "courseTitle": body.get("courseTitle"),
        "videoType": video_type,
        "videoId": video_id,
        "sessionId": session_id,  # <--- 在这里添加
        "startedAt": started_at,
        "originalFilename": meta["originalFilename"],
    }})

    with _get_state_lock(upload_id):
        _save_json(_statefile(upload_id), {"stage": "UPLOADING", "received": 0, "progress": 0})
    return jsonify({"uploadId": upload_id, "objectId": object_id, "exists": False})


@app.route("/api/resolve_session/<int:session_id>", methods=["GET"])
def resolve_session_to_object_id(session_id: int):
    """
    遍历全局状态文件，根据 sessionId 查找对应的 objectId。
    注意：如果 state_schema.json 很大，这个查询会比较慢，但对于几千个视频来说是OK的。
    """
    try:
        with _state_registry_lock:
            all_state = _state_load_all()

        # 遍历所有已知的 objectId
        for object_id, state_data in all_state.items():
            if not isinstance(state_data, dict):
                continue

            # 检查 meta 信息中的 sessionId
            meta = state_data.get("meta", {})
            if meta.get("sessionId") == session_id:
                print(f"[Resolve] 成功将 sessionId {session_id} 解析为 objectId {object_id}")
                return jsonify({
                    "ok": True,
                    "objectId": object_id,
                    "meta": meta
                })

        # 未找到
        return jsonify({"ok": False, "error": "Session ID not found in state file"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =======================================================
# 2. 新增 API：获取 ASR 字幕文件 (JSON 格式)
# =======================================================
@app.route("/api/subtitles/<object_id>", methods=["GET"])
def get_subtitles(object_id: str):
    """
    根据 objectId，从 state_schema.json 中找到 SRT 字幕文件路径，
    并将其作为纯文本（text/plain）返回。
    """
    try:
        state = _state_get(object_id)
        if not state:
            return jsonify({"error": "ObjectId not found in state"}), 404

        # +++ 修改：从 asrPath 改为 srtPath +++
        srt_path = state.get("results", {}).get("srtPath")

        if not srt_path:
            return jsonify({"error": "SRT path not found in state"}), 404

        if not os.path.exists(srt_path):
            return jsonify({"error": "SRT file not found on disk"}), 404

        # +++ 修改：mimetype 改为 text/plain +++
        return send_file(srt_path, mimetype="text/plain")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ingestions/<upload_id>/segments", methods=["POST"])
def put_segment(upload_id: str):
    partsdir = _partsdir(upload_id)
    if not os.path.isdir(partsdir):
        return jsonify({"error": "invalid uploadId"}), 404
    try:
        idx = int(request.args.get("i", "-1"))
    except ValueError:
        return jsonify({"error": "missing or invalid index"}), 400
    if idx < 1:
        return jsonify({"error": "missing index"}), 400

    partname = f"part_{idx:05d}.ts"
    partpath = os.path.join(partsdir, partname)

    if os.path.exists(partpath):
        meta = _load_json(_metafile(upload_id), {}) or {}
        total = int(meta.get("total", 0))
        received = len([n for n in os.listdir(partsdir) if n.startswith("part_")])
        progress = (received / total) if total > 0 else 0
        with _get_state_lock(upload_id):
            _save_json(_statefile(upload_id), {"stage": "UPLOADING", "received": received, "progress": progress})
        return jsonify({"ok": True, "skipped": True, "received": received, "total": total})

    data = request.get_data(cache=False, as_text=False)
    if not data:
        return jsonify({"error": "empty body"}), 400
    with open(partpath, "wb") as f: f.write(data)

    meta = _load_json(_metafile(upload_id), {}) or {}
    total = int(meta.get("total", 0))
    received = len([n for n in os.listdir(partsdir) if n.startswith("part_")])
    progress = (received / total) if total > 0 else 0
    with _get_state_lock(upload_id):
        _save_json(_statefile(upload_id), {"stage": "UPLOADING", "received": received, "progress": progress})
    return jsonify({"ok": True, "received": received, "total": total})

@app.route("/api/ingestions/<upload_id>/missing", methods=["GET"])
def get_missing(upload_id: str):
    partsdir = _partsdir(upload_id)
    if not os.path.isdir(partsdir):
        return jsonify({"missing": []})
    meta = _load_json(_metafile(upload_id), {}) or {}
    total = int(meta.get("total", 0))
    if total <= 0:
        return jsonify({"missing": []})
    existing = set(
        int(n[5:10]) for n in os.listdir(partsdir)
        if n.startswith("part_") and len(n) >= 10 and n[5:10].isdigit()
    )
    missing = [i for i in range(1, total + 1) if i not in existing]
    return jsonify({"missing": missing})

@app.route("/api/ingestions/<upload_id>/complete", methods=["POST"])
def complete(upload_id: str):
    if not os.path.isdir(_partsdir(upload_id)):
        return jsonify({"error": "invalid uploadId"}), 404
    with _get_state_lock(upload_id):
        state = _load_json(_statefile(upload_id), {}) or {}
        state.update({"stage": "QUEUED", "progress": 0})
        _save_json(_statefile(upload_id), state)
    threading.Thread(target=_merge_and_transcode_worker, args=(upload_id,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/ingestions/<upload_id>/status", methods=["GET"])
def status(upload_id: str):
    with _get_state_lock(upload_id):
        state = _load_json(_statefile(upload_id), None)
    if not state:
        return jsonify({"stage": "UNKNOWN", "progress": 0})
    return jsonify(state)

# -------- Download --------
@app.route("/api/download/<object_id>", methods=["GET"])
def download(object_id: str):
    raw = request.args.get("raw")
    if raw == "ts":
        path = _final_ts_by_object(object_id)
        if not os.path.exists(path):
            return jsonify({"error": "not found"}), 404
        return send_file(path, as_attachment=True, download_name=f"{object_id}.ts")
    path = _final_mp4_by_object(object_id)
    if not os.path.exists(path):
        return jsonify({"error": "not found"}), 404
    return send_file(path, as_attachment=True, download_name=f"{object_id}.mp4")

# -------- Search (demo) --------
@app.route('/api/search', methods=['GET'])
def handle_search():
    q = request.args.get('q')
    video_id = request.args.get('videoId')  # 传 objectId
    if not q or not video_id:
        return jsonify({"error": "请求错误，必须同时提供 'q' 和 'videoId' 参数。"}), 400

    if es_indexer is None:
        return jsonify({"error": "搜索不可用：es_indexer 未加载"}), 500

    try:
        index_name = _index_for_object(video_id)

        # 首选：通过索引查询（每视频一个索引，不再跨视频过滤）
        if hasattr(es_indexer, 'get_search_results_by_index'):
            results_list = es_indexer.get_search_results_by_index(q, index_name)
        elif hasattr(es_indexer, 'search_index'):
            results_list = es_indexer.search_index(index_name, q)
        elif hasattr(es_indexer, 'get_search_results'):
            # 兼容旧接口：回退到按 videoId 过滤的旧方法
            results_list = es_indexer.get_search_results(q, video_id, index_name)
        else:
            return jsonify({"error": "es_indexer 缺少查询方法"}), 500

        return jsonify({
            "query": q,
            "video_id": video_id,
            "index": index_name,
            "count": len(results_list),
            "hits": results_list
        })
    except Exception as e:
        print(f"[SEARCH ERROR] {e}", file=sys.stderr)
        return jsonify({"error": "服务器内部错误，搜索失败。"}), 500


# -------- Health --------
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "ts": time.time(),
        "temp_dir": TEMP_UPLOAD_DIR,
        "final_dir": FINAL_VIDEO_DIR
    })

# ===================== Entrypoint =====================
if __name__ == "__main__":
    print("=== Insight Ingestion + Search API ===")
    print(f"TEMP_UPLOAD_DIR : {TEMP_UPLOAD_DIR}")
    print(f"FINAL_VIDEO_DIR : {FINAL_VIDEO_DIR}")
    print("Search:", "ENABLED" if es_indexer else "DISABLED")
    print("Run: http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
