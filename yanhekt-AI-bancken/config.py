# 文件名: config.py
# 职责: 加载 .env 配置文件，并为项目所有模块提供配置。

import os
import sys
from dotenv import load_dotenv

# --- 1. 加载 .env 文件 ---
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if not os.path.exists(dotenv_path):
    print(f"错误：.env 配置文件未找到，请检查路径：{dotenv_path}")
    sys.exit(1)

load_dotenv(dotenv_path)

# --- 2. OSS 配置 ---
OSS_ACCESS_KEY_ID = os.getenv('OSS_TEST_ACCESS_KEY_ID')
OSS_ACCESS_KEY_SECRET = os.getenv('OSS_TEST_ACCESS_KEY_SECRET')
OSS_BUCKET_NAME = os.getenv('OSS_TEST_BUCKET')
OSS_ENDPOINT = os.getenv('OSS_TEST_ENDPOINT')
OSS_ENDPOINT_INTERNAL = os.getenv('OSS_TEST_ENDPOINT_INTERNAL')

# --- 3. 通义听悟 配置 ---
TINGWU_ACCESS_KEY_ID = os.getenv('TINGWU_ACCESS_KEY_ID', OSS_ACCESS_KEY_ID)
TINGWU_ACCESS_KEY_SECRET = os.getenv('TINGWU_ACCESS_KEY_SECRET', OSS_ACCESS_KEY_SECRET)
TINGWU_APP_KEY = os.getenv('TINGWU_APP_KEY')

# --- 4. 【新】Elasticsearch 配置 ---
ES_ENDPOINT = os.getenv('ES_ENDPOINT')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')


# --- 5. 配置校验 ---
# 确保关键配置都已加载
_missing_configs = []
if not OSS_ACCESS_KEY_ID or '<' in OSS_ACCESS_KEY_ID:
    _missing_configs.append("OSS_TEST_ACCESS_KEY_ID")
if not OSS_ACCESS_KEY_SECRET or '<' in OSS_ACCESS_KEY_SECRET:
    _missing_configs.append("OSS_TEST_ACCESS_KEY_SECRET")
if not OSS_BUCKET_NAME:
    _missing_configs.append("OSS_TEST_BUCKET")
if not OSS_ENDPOINT:
    _missing_configs.append("OSS_TEST_ENDPOINT")
if not OSS_ENDPOINT_INTERNAL:
    _missing_configs.append("OSS_TEST_ENDPOINT_INTERNAL")
if not TINGWU_APP_KEY or '<' in TINGWU_APP_KEY:
    _missing_configs.append("TINGWU_APP_KEY")

# 【新】校验 ES 配置
if not ES_ENDPOINT or '<' in ES_ENDPOINT:
    _missing_configs.append("ES_ENDPOINT")
if not ES_USERNAME or '<' in ES_USERNAME:
    _missing_configs.append("ES_USERNAME")
if not ES_PASSWORD or '<' in ES_PASSWORD:
    _missing_configs.append("ES_PASSWORD")


if _missing_configs:
    print("错误：以下配置项在 .env 文件中缺失或未正确填写：")
    for item in _missing_configs:
        print(f" - {item}")
    print("请检查您的 .env 文件。")
    sys.exit(1)

# --- 6. 【新】文件上传路径配置 ---
# (os.path.dirname(__file__) 指向我们 .py 脚本所在的当前目录)
PROJECT_ROOT = os.path.dirname(__file__)

# 1. 分片上传的“临时中转站”
TEMP_UPLOAD_DIR = os.path.join(PROJECT_ROOT, "tmp_uploads")

# 2. 视频“合并”完成后的“最终存放点”
FINAL_VIDEO_DIR = os.path.join(PROJECT_ROOT, "final_videos")
if __name__ == "__main__":
    # 这是一个简单的小测试，您可以直接运行 python config.py 来检查配置是否加载成功
    print("--- 配置加载测试 (V3) ---")
    print("\n--- OSS ---")
    print(f"OSS Key ID: ...{OSS_ACCESS_KEY_ID[-4:]}")
    print(f"OSS Bucket: {OSS_BUCKET_NAME}")
    print(f"OSS Endpoint: {OSS_ENDPOINT}")
    print(f"OSS 内网 Endpoint: {OSS_ENDPOINT_INTERNAL}")
    print("\n--- Tingwu ---")
    print(f"Tingwu AppKey: ...{TINGWU_APP_KEY[-4:]}")
    print("\n--- Elasticsearch ---")
    print(f"ES Endpoint: {ES_ENDPOINT}")
    print(f"ES Username: {ES_USERNAME}")
    print(f"ES Password: ...{ES_PASSWORD[-4:]}")
    print("\n配置加载成功！")