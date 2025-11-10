# 文件名: es_indexer.py
# 职责: 封装所有 Elasticsearch 操作 (连接, 创建索引, 批量索引, 搜索)
# 版本: V8 - 与 app.py 对齐：get_search_results(query, video_id, index_name)

import os
import sys
import json
import time
from datetime import datetime
import warnings

# --- 1) 配置 ---
try:
    import config
except ImportError:
    print("错误：无法导入 'config.py'。请确保与 es_indexer.py 同目录。")
    sys.exit(1)

DEFAULT_INDEX_NAME = getattr(config, "ES_INDEX_NAME", "yanhe-knowledge-base-v1")

# --- 2) 依赖 ---
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    from urllib3.exceptions import InsecureRequestWarning
except ImportError:
    print("错误：需要安装 elasticsearch、urllib3。示例: pip install 'elasticsearch==8.14.0'")
    sys.exit(1)

# --- 3) 连接 ES ---
ES_CLIENT = None
try:
    print("正在连接到 Elasticsearch Serverless (跳过证书验证)...")
    warnings.simplefilter('ignore', InsecureRequestWarning)
    ES_CLIENT = Elasticsearch(
        [config.ES_ENDPOINT],
        basic_auth=(config.ES_USERNAME, config.ES_PASSWORD),
        verify_certs=False,
    )
    if not ES_CLIENT.options(request_timeout=30).ping():
        raise RuntimeError("Elasticsearch ping 失败")
    print("Elasticsearch 连接成功！")
except Exception as e:
    print(f"错误：Elasticsearch 连接失败: {e}")
    sys.exit(1)

# --- 4) 创建索引 ---
def create_index_if_not_exists(index_name: str = DEFAULT_INDEX_NAME) -> None:
    """
    创建索引（如不存在）。映射中对 content 使用 ik_smart（若不支持需改 standard）。
    """
    mapping = {
        "mappings": {
            "properties": {
                "video_id": {"type": "keyword"},
                "type": {"type": "keyword"},  # ASR / PPT
                "content": {
                    "type": "text",
                    "analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                },
                "start_time_ms": {"type": "long"},
                "end_time_ms": {"type": "long"},
                "metadata": {"type": "object", "enabled": False},
            }
        }
    }
    try:
        ES_CLIENT.indices.create(index=index_name, body=mapping)
        print(f"索引 '{index_name}' 创建成功")
    except Exception as e:
        s = str(e)
        if "resource_already_exists_exception" in s:
            print(f"索引 '{index_name}' 已存在，跳过创建")
        elif "analyzer [ik_smart] not found" in s:
            print("【重要】ES 不支持 ik_smart，请把映射中的 analyzer 改为 'standard' 后重试。")
            raise
        else:
            print(f"创建索引异常: {e}")
            raise

# --- 5) 批量索引 ---
def bulk_index_cards(cards_list, index_name: str = DEFAULT_INDEX_NAME) -> bool:
    """
    批量索引卡片。cards_list 形如：
    {
      "video_id": "<objectId>",
      "type": "ASR"|"PPT",
      "content": "文本",
      "start_time_ms": 123,
      "end_time_ms": 456,
      "metadata": {...}
    }
    """
    if not cards_list:
        print("没有要索引的卡片")
        return False

    actions = ({"_op_type": "index", "_index": index_name, "_source": doc} for doc in cards_list)
    try:
        success, errors = bulk(ES_CLIENT.options(request_timeout=60), actions)
        print(f"批量索引：成功 {success} 条")
        if errors:
            print(f"批量索引存在错误：{len(errors)} 条")
            return False
        return True
    except Exception as e:
        print(f"批量索引异常: {e}")
        return False

# --- 6) 搜索 ---
def _execute_search(text_query: str, video_id: str | None, index_name: str = DEFAULT_INDEX_NAME):
    """
    内部搜索：match content + 可选 term(video_id) 过滤；按 start_time_ms 升序。
    """
    must_clauses = [
        {"match": {"content": {"query": text_query, "analyzer": "ik_smart"}}}
    ]
    filter_clauses = []
    if video_id:
        filter_clauses.append({"term": {"video_id": video_id}})

    body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses,
            }
        },
        "size": 10,
        "sort": [{"start_time_ms": "asc"}],
    }
    try:
        resp = ES_CLIENT.options(request_timeout=30).search(index=index_name, body=body)
        return resp.get("hits", {}).get("hits", []), resp.get("took", 0)
    except Exception as e:
        print(f"执行搜索异常: {e}")
        return [], 0

def get_search_results(text_query: str, video_id: str | None, index_name: str = DEFAULT_INDEX_NAME):
    """
    给 app.py 调用。按 video_id 精确过滤，返回“精简卡片列表”：
      [{ type, score, start_ms, time_str, content }]
    """
    hits, took_ms = _execute_search(text_query, video_id, index_name)
    results = []
    for h in hits:
        src = h.get("_source", {}) or {}
        start_ms = int(src.get("start_time_ms", 0))
        time_str = datetime.utcfromtimestamp(max(start_ms, 0) / 1000).strftime("%H:%M:%S")
        results.append({
            "type": src.get("type"),
            "score": h.get("_score"),
            "start_ms": start_ms,
            "time_str": time_str,
            "content": src.get("content") or "",
        })
    return results

# --- 7) 测试入口（可选） ---
def search_content_for_testing(text_query: str, video_id: str | None = None, index_name: str = DEFAULT_INDEX_NAME):
    print(f"\n[测试搜索] q='{text_query}' video_id='{video_id or '*'}'")
    hits, took = _execute_search(text_query, video_id, index_name)
    print(f"匹配 {len(hits)} 条 (took {took} ms)")
    for i, h in enumerate(hits, 1):
        s = h.get("_source", {}) or {}
        ms = int(s.get("start_time_ms", 0))
        ts = datetime.utcfromtimestamp(max(ms, 0)/1000).strftime("%H:%M:%S")
        print(f"- #{i} [{s.get('type')}] @{ts}  { (s.get('content') or '')[:80] }")

if __name__ == "__main__":
    # 简单自测（需要本地已建索引）
    create_index_if_not_exists(DEFAULT_INDEX_NAME)
    time.sleep(1)
    search_content_for_testing("B+树", video_id=None)
