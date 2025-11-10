# 文件名: oss_uploader.py
# 职责: 封装所有阿里云 OSS 操作（初始化、存在性检查、断点续传上传、内网签名URL、公网直链、删除）
# 说明:
#   - 使用两个 Bucket 句柄：public_bucket(公网上传/删除/直链) + internal_bucket(仅用于生成内网签名URL)
#   - 提供 exists() 幂等检测；upload_file_with_progress() 可在 exists 且不覆盖时跳过
#   - 兼容你的 config.py；若缺项会给出友好错误

from __future__ import annotations

import os
import sys
import math
import oss2
from typing import Optional, Dict
from urllib.parse import urlparse

# ---------- 1) 加载配置 ----------
try:
    import config
except ImportError:
    print("错误：无法导入 'config.py'，请确认该文件与 oss_uploader.py 同目录。")
    sys.exit(1)

def _require(name: str) -> str:
    v = getattr(config, name, None)
    if not v:
        print(f"错误：config.{name} 未配置。")
        sys.exit(1)
    return v

OSS_ACCESS_KEY_ID     = _require("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = _require("OSS_ACCESS_KEY_SECRET")
OSS_BUCKET_NAME       = _require("OSS_BUCKET_NAME")
OSS_ENDPOINT          = _require("OSS_ENDPOINT")           # 例: https://oss-cn-beijing.aliyuncs.com
OSS_ENDPOINT_INTERNAL = _require("OSS_ENDPOINT_INTERNAL")  # 例: http://oss-cn-beijing-internal.aliyuncs.com

# 可选：统一前缀，便于分门别类（例如 "insight" / "tw-ingestion"）
OSS_KEY_PREFIX = getattr(config, "OSS_KEY_PREFIX", "").strip().strip("/")

# 分片大小(断点续传)；可按需要在调用层覆盖
DEFAULT_PART_SIZE = getattr(config, "OSS_PART_SIZE", 8 * 1024 * 1024)  # 8MB

# ---------- 2) 初始化 Bucket ----------
try:
    _auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    public_bucket   = oss2.Bucket(_auth, OSS_ENDPOINT,          OSS_BUCKET_NAME)
    internal_bucket = oss2.Bucket(_auth, OSS_ENDPOINT_INTERNAL, OSS_BUCKET_NAME)

    # 简单连通性检查（公网）
    public_bucket.get_bucket_info()
except oss2.exceptions.OssError as e:
    print("错误：OSS 初始化失败，请检查配置与网络：", e)
    sys.exit(1)
except Exception as e:
    print("错误：OSS 初始化时发生未知异常：", e)
    sys.exit(1)

# ---------- 3) 工具函数 ----------
def _join_key(*parts: str) -> str:
    """
    拼接 OSS Key，自动插入可选前缀 OSS_KEY_PREFIX。
    传入的分段会去掉多余的 '/'，最终返回形如 'prefix/a/b/file.ext'
    """
    segs = []
    if OSS_KEY_PREFIX:
        segs.append(OSS_KEY_PREFIX)
    for p in parts:
        p = (p or "").strip().strip("/")
        if p:
            segs.append(p)
    return "/".join(segs)

def get_internal_signed_url_for_tingwu(remote_key, expires_sec=3600):
    """
    为通义听悟生成【内网】签名 URL（北京区内网，免公网流量）。
    """
    return internal_bucket.sign_url('GET', remote_key, expires_sec)

def build_video_key(object_id: str, ext: str = "mp4", folder: str = "final-videos") -> str:
    """
    依据 objectId 生成一个规范的 Key。你也可以在上层自行传 remote_key，此函数只是个便捷封装。
    默认: <PREFIX>/final-videos/<object_id>.mp4
    """
    filename = f"{object_id}.{ext.lstrip('.')}"
    return _join_key(folder, filename)

def _ensure_scheme(url: str) -> str:
    return url if url.startswith(("http://", "https://")) else "https://" + url

def build_public_url(key: str) -> str:
    """
    构造公网直链（非签名）。注意：若 Bucket 私有，此 URL 不能直接访问；用来展示“理论路径”或调试。
    """
    parsed = urlparse(_ensure_scheme(OSS_ENDPOINT))
    host = parsed.netloc or parsed.path  # 兼容不带 scheme 的写法
    return f"{parsed.scheme}://{OSS_BUCKET_NAME}.{host}/{key.lstrip('/')}"

# ---------- 4) 基础操作 ----------
def exists(remote_key: str) -> bool:
    """
    基于 HEAD 判断对象是否存在。
    """
    try:
        public_bucket.head_object(remote_key)
        return True
    except oss2.exceptions.NoSuchKey:
        return False
    except oss2.exceptions.OssError:
        # 其他 OSS 错误统一当作不存在处理（调用方可选择重试/上报）
        return False

def upload_file_with_progress(
    local_path: str,
    remote_key: str,
    *,
    overwrite: bool = False,
    part_size: int = DEFAULT_PART_SIZE,
) -> Dict[str, object]:
    """
    断点续传 + 进度回调（打印到 stdout）。
    返回: { "ok": bool, "skipped": bool, "key": str, "size": int, "etag": Optional[str] }
    - 若 overwrite=False 且 exists=True，则直接跳过上传（skipped=True）
    """
    if not os.path.exists(local_path):
        return {"ok": False, "skipped": False, "key": remote_key, "size": 0, "etag": None, "error": "local file not found"}

    fsize = os.path.getsize(local_path)

    if not overwrite and exists(remote_key):
        print(f"[OSS] 已存在，跳过上传: {remote_key}")
        return {"ok": True, "skipped": True, "key": remote_key, "size": fsize, "etag": None}

    consumed_last = 0

    def _progress(consumed_bytes, total_bytes):
        nonlocal consumed_last
        if total_bytes:
            # 避免过于频繁刷新
            step = total_bytes // 100 if total_bytes >= 100 else 1
            if consumed_bytes - consumed_last >= step or consumed_bytes == total_bytes:
                rate = int(100 * consumed_bytes / total_bytes)
                print(f"\r[OSS] 上传进度: {rate:3d}% ({consumed_bytes}/{total_bytes} bytes)", end="")
                consumed_last = consumed_bytes
                if consumed_bytes == total_bytes:
                    print()

    try:
        # resumable_upload 支持断点续传；默认 checkpoint 临时文件放在当前目录
        result = oss2.resumable_upload(
            public_bucket,
            remote_key,
            local_path,
            part_size=part_size,
            progress_callback=_progress,
        )
        etag = getattr(result, "etag", None)
        print(f"[OSS] 上传成功: {remote_key} (size={fsize}, etag={etag})")
        return {"ok": True, "skipped": False, "key": remote_key, "size": fsize, "etag": etag}
    except oss2.exceptions.OssError as e:
        print(f"[OSS] 上传失败: {remote_key} -> {e}")
        return {"ok": False, "skipped": False, "key": remote_key, "size": fsize, "etag": None, "error": str(e)}
    except Exception as e:
        print(f"[OSS] 上传异常: {remote_key} -> {e}")
        return {"ok": False, "skipped": False, "key": remote_key, "size": fsize, "etag": None, "error": str(e)}

def sign_internal_url(remote_key: str, expires_sec: int = 3600, method: str = "GET") -> Optional[str]:
    """
    生成【内网】签名 URL（给通义听悟用）。本机只生成 URL，不需要能访问内网。
    """
    try:
        url = internal_bucket.sign_url(method, remote_key, expires_sec)
        # 保险处理：确保是内网域名
        if "internal" not in url:
            print("[OSS] 警告：生成的URL不包含 'internal'，请检查 OSS_ENDPOINT_INTERNAL 是否为内网域。")
        return url
    except Exception as e:
        print(f"[OSS] 内网签名URL生成失败: {e}")
        return None

def delete_object(remote_key: str) -> bool:
    """
    删除 OSS 对象（用于 Tingwu 完成后的清理）。
    """
    try:
        public_bucket.delete_object(remote_key)
        print(f"[OSS] 删除成功: {remote_key}")
        return True
    except Exception as e:
        print(f"[OSS] 删除失败: {remote_key} -> {e}")
        return False

# ---------- 5) 命令行简单测试 ----------
if __name__ == "__main__":
    """
    用法示例（按需改动）：
      1) 设置下面的 LOCAL_PATH 与 OBJECT_ID；
      2) 运行: python oss_uploader.py
    """
    # --- 示例参数（请按需修改/注释） ---
    LOCAL_PATH = ""  # 比如: r"D:\path\to\final_videos\0cc6a679f34b6277.mp4"
    OBJECT_ID  = ""  # 比如: "0cc6a679f34b6277"
    # -------------------------------

    if not LOCAL_PATH or not OBJECT_ID:
        print("自检：未设置 LOCAL_PATH/OBJECT_ID，仅做连通性测试。")
        print("公网直链示例：", build_public_url("test-uploads/dummy.txt"))
        sys.exit(0)

    key = build_video_key(OBJECT_ID, ext="mp4", folder="final-videos")
    print("[Test] 目标 Key:", key)

    if exists(key):
        print("[Test] 远端已存在，跳过上传。")
    else:
        print("[Test] 执行上传...")
        r = upload_file_with_progress(LOCAL_PATH, key, overwrite=False)
        if not r.get("ok"):
            print("[Test] 上传失败：", r)
            sys.exit(1)

    print("[Test] 生成内网签名URL（给通义听悟用）...")
    s = sign_internal_url(key, expires_sec=3600)
    if s:
        print("[Signed Internal URL]\n", s)
    else:
        print("签名失败。")
