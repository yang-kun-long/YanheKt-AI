# 文件名: tingwu_analyzer.py
# 职责: 通义听悟 API 封装 + 基于 OSS Key 的提交流程 + 结果下载/落地 +（可选）本地 SRT/知识卡片生成与 ES 索引
# 依赖: config.py, aliyun-python-sdk-core, requests
# 可选依赖: oss_uploader.py, json_transformer.py, es_indexer.py（存在则启用对应功能）

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Dict, Any

import requests

# ========== 1) 配置 ==========
try:
    import config
except ImportError:
    print("错误：无法导入 config.py。请确保与本文件同目录。")
    sys.exit(1)

# 控制项（也可放到 config.py）
RESULTS_DIR     = getattr(config, "RESULTS_DIR", "analysis_results")
POLL_INTERVAL_S = getattr(config, "TINGWU_POLL_INTERVAL", 8)       # 轮询间隔
POLL_TIMEOUT_S  = getattr(config, "TINGWU_POLL_TIMEOUT", 2*60*60)  # 超时 2h
DELETE_OSS_AFTER_SUCCESS = getattr(config, "TINGWU_DELETE_OSS_AFTER", True)

# ========== 2) 阿里云 SDK ==========
try:
    from aliyunsdkcore.client import AcsClient
    from aliyunsdkcore.request import CommonRequest
    from aliyunsdkcore.auth.credentials import AccessKeyCredential
except ImportError:
    print("错误：未安装 aliyun-python-sdk-core。请运行: pip install aliyun-python-sdk-core")
    sys.exit(1)

# 初始化 Client
try:
    _cred = AccessKeyCredential(config.TINGWU_ACCESS_KEY_ID, config.TINGWU_ACCESS_KEY_SECRET)
    _REGION = 'cn-beijing'
    _DOMAIN = 'tingwu.cn-beijing.aliyuncs.com'
    _client = AcsClient(region_id=_REGION, credential=_cred)
except Exception as e:
    print(f"错误：初始化 Tingwu Client 失败：{e}")
    sys.exit(1)

# ========== 3) 可选模块 ==========
try:
    import oss_uploader
except Exception:
    oss_uploader = None

try:
    import json_transformer
except Exception:
    json_transformer = None

try:
    import es_indexer
except Exception:
    es_indexer = None


# ========== 4) 基础 API ==========
def _new_request(domain: str, version: str, protocol: str, method: str, uri: str) -> CommonRequest:
    req = CommonRequest()
    req.set_accept_format('json')
    req.set_domain(domain)
    req.set_version(version)
    req.set_protocol_type(protocol)
    req.set_method(method)
    req.set_uri_pattern(uri)
    req.add_header('Content-Type', 'application/json')
    return req


def submit_transcription_task(file_url: str) -> Optional[str]:
    """
    直接用 URL 提交离线任务。成功返回 task_id。
    """
    try:
        body: Dict[str, Any] = {
            "AppKey": config.TINGWU_APP_KEY,
            "Input": {
                "SourceLanguage": "auto",
                "FileUrl": file_url,
            },
            "Parameters": {
                "Transcription": {"DiarizationEnabled": True},
                "PptExtractionEnabled": True
            }
        }
        req = _new_request(_DOMAIN, "2023-09-30", "https", "PUT", "/openapi/tingwu/v2/tasks")
        req.add_query_param("type", "offline")
        req.set_content(json.dumps(body, ensure_ascii=False).encode("utf-8"))

        print(f"[Tingwu] 提交任务: {file_url[:80]}...")
        resp = _client.do_action_with_exception(req)
        data = json.loads(resp)

        if data.get("Code") == "0" and data.get("Data"):
            task_id = data["Data"].get("TaskId")
            print(f"[Tingwu] 提交成功: TaskId={task_id}")
            return task_id
        print(f"[Tingwu] 提交失败: {data}")
        return None
    except Exception as e:
        print(f"[Tingwu] 提交异常: {e}")
        return None


def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """
    查询任务状态与结果（原始 Data）。
    """
    try:
        uri = f"/openapi/tingwu/v2/tasks/{task_id}"
        req = _new_request(_DOMAIN, "2023-09-30", "https", "GET", uri)
        resp = _client.do_action_with_exception(req)
        data = json.loads(resp)
        return data.get("Data")
    except Exception as e:
        print(f"[Tingwu] 查询异常: {e}")
        return None


def poll_for_result(task_id: str, interval_s: int = POLL_INTERVAL_S, timeout_s: int = POLL_TIMEOUT_S) -> Optional[Dict[str, Any]]:
    """
    轮询直到 COMPLETED/FAILED/超时。返回 Data。
    """
    print(f"[Tingwu] 开始轮询 Task: {task_id} (每 {interval_s}s, 超时 {timeout_s}s)")
    t0 = time.time()
    while True:
        if time.time() - t0 > timeout_s:
            print("[Tingwu] 轮询超时")
            return None

        data = get_task_status(task_id)
        if not data:
            time.sleep(interval_s)
            continue

        status = data.get("TaskStatus")
        if status == "COMPLETED":
            print("[Tingwu] 任务完成")
            return data
        if status == "FAILED":
            print("[Tingwu] 任务失败: ", data.get("ErrorMessage"))
            return data

        # PENDING/ONGOING/其他
        print(f"\r[Tingwu] 进行中... status={status}", end="")
        time.sleep(interval_s)


# ========== 5) 结果下载 ==========
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def download_result_json(url: str, save_as: str) -> Optional[Dict[str, Any]]:
    try:
        _ensure_dir(os.path.dirname(save_as))
        print(f"[Tingwu] 下载 JSON: {url[:80]}...")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(save_as, "w", encoding="utf-8") as f:
            f.write(r.text)
        return json.loads(r.text)
    except Exception as e:
        print(f"[Tingwu] 下载JSON失败: {e}")
        return None

def download_binary(url: str, save_as: str) -> bool:
    try:
        _ensure_dir(os.path.dirname(save_as))
        print(f"[Tingwu] 下载文件: {url[:80]} -> {save_as}")
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        with open(save_as, "wb") as f:
            f.write(r.content)
        return True
    except Exception as e:
        print(f"[Tingwu] 下载二进制失败: {e}")
        return False


# ========== 6) 高阶：基于 OSS Key 的完整流程 ==========
def submit_task_for_oss_key(oss_key: str, url_ttl: int = 3600) -> Optional[str]:
    """
    使用内网签名URL(通过 oss_uploader)为指定 oss_key 提交听悟任务。
    """
    if not oss_uploader:
        print("[Tingwu] 错误: 未加载 oss_uploader，无法生成内网签名URL")
        return None
    url = oss_uploader.sign_internal_url(oss_key, expires_sec=url_ttl, method="GET")
    if not url:
        print("[Tingwu] 内网URL生成失败")
        return None
    return submit_transcription_task(url)


def fetch_and_store_results(task_id: str, base_dir: str = RESULTS_DIR) -> Dict[str, Any]:
    """
    轮询 -> 下载结果(ASR/PPT JSON + 可选 PDF)。
    返回:
      {
        "ok": bool,
        "status": "COMPLETED"/"FAILED"/"TIMEOUT"/"ERROR",
        "task_id": task_id,
        "asr_json": "<path or None>",
        "ppt_json": "<path or None>",
        "pdf": "<path or None>",
        "raw": <原始 Data 或 错误>
      }
    """
    _ensure_dir(base_dir)
    data = poll_for_result(task_id)
    if not data:
        return {"ok": False, "status": "TIMEOUT", "task_id": task_id, "asr_json": None, "ppt_json": None, "pdf": None, "raw": None}

    status = data.get("TaskStatus")
    if status != "COMPLETED":
        return {"ok": False, "status": status or "FAILED", "task_id": task_id, "asr_json": None, "ppt_json": None, "pdf": None, "raw": data}

    result = data.get("Result", {}) or {}
    asr_url = result.get("Transcription")
    ppt_url = result.get("PptExtraction")

    asr_path = os.path.join(base_dir, f"{task_id}_ASR_Result.json") if asr_url else None
    ppt_path = os.path.join(base_dir, f"{task_id}_PPT_Result.json") if ppt_url else None
    pdf_path = None

    if asr_url:
        j = download_result_json(asr_url, asr_path)
        if j is None:
            asr_path = None

    if ppt_url:
        j = download_result_json(ppt_url, ppt_path)
        if j:
            pdf_url = (j.get("PptExtraction") or {}).get("PdfPath")
            if pdf_url:
                pdf_path = os.path.join(base_dir, f"{task_id}_PPT_Result.pdf")
                if not download_binary(pdf_url, pdf_path):
                    pdf_path = None
        else:
            ppt_path = None

    return {
        "ok": True,
        "status": "COMPLETED",
        "task_id": task_id,
        "asr_json": asr_path,
        "ppt_json": ppt_path,
        "pdf": pdf_path,
        "raw": data
    }


# ========== 7) 可选：本地生成 SRT / 解析卡片 / ES 索引 ==========
def postprocess_locally(object_id: str, asr_json_path: Optional[str], ppt_json_path: Optional[str]) -> Dict[str, Any]:
    """
    若安装了 json_transformer / es_indexer，则:
      - 生成 SRT（同目录）
      - 解析 ASR/PPT 成卡片
      - 批量索引到 ES（若可用）
    """
    result: Dict[str, Any] = {
        "srt": None,
        "asr_cards": 0,
        "ppt_cards": 0,
        "indexed": False
    }

    if not json_transformer:
        print("[Post] 未加载 json_transformer，跳过本地生成与解析")
        return result

    asr_cards = []
    ppt_cards = []
    try:
        if asr_json_path and os.path.exists(asr_json_path):
            srt_path = os.path.join(os.path.dirname(asr_json_path), f"{object_id}.srt")
            ok = json_transformer.generate_srt_file(asr_json_path, srt_path)
            result["srt"] = srt_path if ok else None

            asr_cards = json_transformer.parse_asr_json_to_cards(asr_json_path, object_id)
            result["asr_cards"] = len(asr_cards)

        if ppt_json_path and os.path.exists(ppt_json_path):
            ppt_cards = json_transformer.parse_ppt_json_to_cards(ppt_json_path, object_id)
            result["ppt_cards"] = len(ppt_cards)
    except Exception as e:
        print("[Post] 本地处理异常：", e)

    if es_indexer and (asr_cards or ppt_cards):
        try:
            es_indexer.create_index_if_not_exists(es_indexer.DEFAULT_INDEX_NAME)
            ok = es_indexer.bulk_index_cards(asr_cards + ppt_cards, es_indexer.DEFAULT_INDEX_NAME)
            result["indexed"] = bool(ok)
        except Exception as e:
            print("[Post] ES 索引异常：", e)

    return result


# ========== 8) 一体化：供 app.py 直接调用 ==========
def run_tingwu_pipeline_for_oss(object_id: str, oss_key: str, *,
                                delete_oss_after: bool = DELETE_OSS_AFTER_SUCCESS,
                                url_ttl: int = 3600) -> Dict[str, Any]:
    """
    （给 app.py 调用）
    步骤：
      1) 用 oss_key 生成内网签名URL并提交任务
      2) 轮询直到完成
      3) 下载结果到 RESULTS_DIR
      4) （可选）删除 OSS 源文件
      5) （可选）本地后处理 + ES 索引
    返回统一结构，便于写入 state.json：
      {
        "ok": bool,
        "stage": "TINGWU_DONE"/"TINGWU_FAILED"/"TINGWU_ERROR",
        "task_id": str|None,
        "object_id": str,
        "asr_json": path|None,
        "ppt_json": path|None,
        "pdf": path|None,
        "post": {...},
        "message": str|None
      }
    """
    try:
        task_id = submit_task_for_oss_key(oss_key, url_ttl=url_ttl)
        if not task_id:
            return {"ok": False, "stage": "TINGWU_FAILED", "task_id": None, "object_id": object_id,
                    "asr_json": None, "ppt_json": None, "pdf": None, "post": {}, "message": "提交任务失败"}

        res = fetch_and_store_results(task_id, base_dir=RESULTS_DIR)
        if not res.get("ok"):
            return {"ok": False, "stage": "TINGWU_FAILED", "task_id": task_id, "object_id": object_id,
                    "asr_json": None, "ppt_json": None, "pdf": None, "post": {}, "message": f"任务未完成: {res.get('status')}"}

        # 可选：删除 OSS 源文件
        if delete_oss_after and oss_uploader:
            try:
                oss_uploader.delete_object(oss_key)
            except Exception as e:
                print("[Tingwu] 删除 OSS 源失败：", e)

        # 可选：本地后处理 + ES 索引
        post = postprocess_locally(object_id, res.get("asr_json"), res.get("ppt_json"))

        return {
            "ok": True,
            "stage": "TINGWU_DONE",
            "task_id": task_id,
            "object_id": object_id,
            "asr_json": res.get("asr_json"),
            "ppt_json": res.get("ppt_json"),
            "pdf": res.get("pdf"),
            "post": post,
            "message": None
        }
    except Exception as e:
        return {"ok": False, "stage": "TINGWU_ERROR", "task_id": None, "object_id": object_id,
                "asr_json": None, "ppt_json": None, "pdf": None, "post": {}, "message": str(e)}


# ========== 9) 模块自测 ==========
if __name__ == "__main__":
    # 自测一：已有 TaskId 轮询 + 下载
    TEST_TASK_ID = ""  # 可填历史 TaskId
    if TEST_TASK_ID:
        out = fetch_and_store_results(TEST_TASK_ID)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        sys.exit(0)

    # 自测二：基于 oss_key 全流程（需先在 OSS 放一个 mp4/ts）
    TEST_OBJECT_ID = ""   # 如 "0cc6a679f34b6277"
    TEST_OSS_KEY   = ""   # 如 "final-videos/0cc6a679f34b6277.mp4" 或含前缀 "insight/final-videos/.."

    if TEST_OBJECT_ID and TEST_OSS_KEY:
        out = run_tingwu_pipeline_for_oss(TEST_OBJECT_ID, TEST_OSS_KEY, delete_oss_after=False)
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print("自测提示：请设置 TEST_TASK_ID 或 (TEST_OBJECT_ID & TEST_OSS_KEY) 以运行。")
