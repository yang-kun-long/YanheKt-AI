# 文件名: json_transformer.py
# 职责: 数据的“转换层”。(V3.2 - 阿里云 OCR 完美替换版)
#       1. 解析 ASR JSON -> 知识卡片 (用于 ES)
#       2. 解析 ASR JSON -> SRT 字幕文件 (用于前端)
#       3. 解析 PPT JSON -> 多线程【阿里云OCR】 -> 知识卡片 (用于 ES)
#       本模块不依赖 OSS 或 ES。

import os
import sys
import json
# requests 和 io 不再需要，但保留以防万一
import requests
import io
import time  # 用于测试OCR速度
import concurrent.futures  # 【新】导入多线程库

# --- 1. 【新】初始化 阿里云 OCR 客户端 ---
try:
    # 导入您的配置文件
    import config
    # 导入阿里云 SDK
    from alibabacloud_ocr_api20210707.client import Client as OcrClient
    from alibabacloud_ocr_api20210707.models import RecognizeAllTextRequest
    from alibabacloud_tea_openapi.models import Config as ApiConfig
    from alibabacloud_tea_util.models import RuntimeOptions
    from alibabacloud_tea_util import client as UtilClient

    print("正在初始化 Aliyun OCR 客户端...")
    # 使用 config.py 中的配置初始化
    client_config = ApiConfig(
        access_key_id=config.OSS_ACCESS_KEY_ID,  # 复用 OSS 的 Key
        access_key_secret=config.OSS_ACCESS_KEY_SECRET
    )
    # 指定 OCR 服务的接入点 (Endpoint)
    client_config.endpoint = 'ocr-api.cn-hangzhou.aliyuncs.com'

    # 创建全局客户端实例
    ALIYUN_OCR_CLIENT = OcrClient(client_config)
    # 创建全局运行时选项 (避免重复创建)
    ALIYUN_OCR_RUNTIME = RuntimeOptions()
    print("Aliyun OCR 客户端初始化完成。")

except ImportError:
    print("错误：未找到 'config.py' 或 'alibabacloud' SDK。")
    print("请确保 config.py 与此文件在同一目录，并已安装 SDK:")
    print("pip install alibabacloud_ocr_api20210707")
    sys.exit(1)
except Exception as e:
    print(f"错误：Aliyun OCR 客户端初始化失败: {e}")
    sys.exit(1)


# --- 2. 帮助函数 ---

def _milliseconds_to_srt_time(ms):
    """
    辅助函数：将毫秒 (例如 47050) 转换为 SRT 时间格式 (00:00:47,050)。
    """
    seconds, milliseconds = divmod(ms, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"


# (已移除 _download_image_to_memory 和 _run_local_ocr)

def _run_aliyun_ocr(image_url: str) -> str:
    """
    【新】
    辅助函数：对给定的 URL 执行 Aliyun OCR。
    在线程中失败时返回空字符串，以匹配原 _run_local_ocr 的行为。
    """
    if not ALIYUN_OCR_CLIENT:
        return ""

    request = RecognizeAllTextRequest()
    request.url = image_url
    request.type = "General"  # 固定使用“通用文字识别”

    try:
        response = ALIYUN_OCR_CLIENT.recognize_all_text_with_options(request, ALIYUN_OCR_RUNTIME)

        if response and response.body and response.body.data:
            return response.body.data.content
        else:
            return ""  # 失败时返回空字符串

    except Exception as e:
        # 在多线程中，我们不希望单个失败导致崩溃，只返回空字符串
        return ""


def _process_one_frame(frame_data):
    """
    【V3.2 - Aliyun 版】
    辅助函数：在【单个线程】中处理【单个PPT帧】(Aliyun OCR -> 格式化)
    输入/输出与原版 _process_one_frame 兼容。
    """
    video_id, frame, index, total = frame_data

    # \r 回到行首刷新，end="" 不换行，实现动态更新
    print(f"\r  > [并发-Aliyun] 正在处理 PPT 帧 {index + 1}/{total}...", end="")

    image_url = frame.get('FileUrl')
    ai_summary = frame.get('Summary', '')

    # --- 核心替换 ---
    ocr_text = ""
    if image_url:
        # 不再下载，直接将 URL 传给阿里云
        ocr_text = _run_aliyun_ocr(image_url)
    # --- 替换结束 ---

    # 合并“AI摘要”和“OCR原始文字”
    full_content = f"{ai_summary}\n{ocr_text}"

    card = {
        "video_id": video_id,
        "type": "PPT",
        "content": full_content,  # 【关键】使用合并后的内容
        "start_time_ms": frame.get('Start', 0),
        "end_time_ms": frame.get('End', 0),
        "metadata": {
            "image_url": image_url,
            "ai_summary": ai_summary,
            "id": frame.get('Id', 0)
        }
    }
    return card


# --- 3. 核心功能函数 (保持原样) ---

def generate_srt_file(asr_json_path, srt_save_path):
    """
    功能一 (您的新需求)：解析 ASR JSON，生成一个 .srt 字幕文件。
    """
    print(f"开始生成 SRT 字幕文件: {srt_save_path}")

    try:
        with open(asr_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        transcription_data = data.get('Transcription', {})
        paragraphs = transcription_data.get('Paragraphs', [])

        if not paragraphs:
            print("警告：ASR JSON 中未找到 'Transcription.Paragraphs' 数据。")
            return False

        srt_content = []
        subtitle_index = 1

        for para in paragraphs:
            words = para.get('Words', [])
            if not words: continue

            current_sentence_text = []
            current_sentence_start = 0
            current_sentence_end = 0
            current_sentence_id = -1

            for word in words:
                sentence_id = word.get('SentenceId')
                if sentence_id != current_sentence_id:
                    if current_sentence_text:
                        start_time_str = _milliseconds_to_srt_time(current_sentence_start)
                        end_time_str = _milliseconds_to_srt_time(current_sentence_end)
                        srt_content.append(str(subtitle_index))
                        srt_content.append(f"{start_time_str} --> {end_time_str}")
                        srt_content.append("".join(current_sentence_text))
                        srt_content.append("")
                        subtitle_index += 1
                    current_sentence_text = [word.get('Text', '')]
                    current_sentence_start = word.get('Start', 0)
                    current_sentence_end = word.get('End', 0)
                    current_sentence_id = sentence_id
                else:
                    current_sentence_text.append(word.get('Text', ''))
                    current_sentence_end = word.get('End', 0)

            if current_sentence_text:
                start_time_str = _milliseconds_to_srt_time(current_sentence_start)
                end_time_str = _milliseconds_to_srt_time(current_sentence_end)
                srt_content.append(str(subtitle_index))
                srt_content.append(f"{start_time_str} --> {end_time_str}")
                srt_content.append("".join(current_sentence_text))
                srt_content.append("")
                subtitle_index += 1

        with open(srt_save_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(srt_content))

        print(f"\nSRT 文件生成成功：共 {subtitle_index - 1} 条字幕。")
        return True

    except Exception as e:
        print(f"生成 SRT 文件时发生未知错误: {e}")
        return False


def parse_asr_json_to_cards(asr_json_path, video_id):
    """
    功能二：解析 ASR (语音) JSON 文件。
    将其聚合成“句子”列表，并转换为“知识卡片” (用于ES)。
    """
    print(f"开始解析 ASR JSON -> 知识卡片: {asr_json_path}")
    knowledge_cards = []

    try:
        with open(asr_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        transcription_data = data.get('Transcription', {})
        paragraphs = transcription_data.get('Paragraphs', [])

        if not paragraphs:
            print("警告：ASR JSON 中未找到 'Transcription.Paragraphs' 数据。")
            return []

        for para in paragraphs:
            speaker_id = para.get('SpeakerId', '0')
            words = para.get('Words', [])
            if not words: continue

            current_sentence = {'text': [], 'start': 0, 'end': 0, 'sentence_id': -1}
            for word in words:
                sentence_id = word.get('SentenceId')
                if sentence_id != current_sentence['sentence_id']:
                    if current_sentence['text']:
                        card = {
                            "video_id": video_id,
                            "type": "ASR",
                            "content": "".join(current_sentence['text']),
                            "start_time_ms": current_sentence['start'],
                            "end_time_ms": current_sentence['end'],
                            "metadata": {"speaker_id": speaker_id}
                        }
                        knowledge_cards.append(card)
                    current_sentence = {'text': [word.get('Text', '')], 'start': word.get('Start', 0),
                                        'end': word.get('End', 0), 'sentence_id': sentence_id}
                else:
                    current_sentence['text'].append(word.get('Text', ''))
                    current_sentence['end'] = word.get('End', 0)

            if current_sentence['text']:
                card = {
                    "video_id": video_id,
                    "type": "ASR",
                    "content": "".join(current_sentence['text']),
                    "start_time_ms": current_sentence['start'],
                    "end_time_ms": current_sentence['end'],
                    "metadata": {"speaker_id": speaker_id}
                }
                knowledge_cards.append(card)

        print(f"ASR 解析完毕：生成 {len(knowledge_cards)} 张“知识卡片”。")
        return knowledge_cards

    except Exception as e:
        print(f"解析 ASR JSON 时发生未知错误: {e}")
        return []


def parse_ppt_json_to_cards(ppt_json_path, video_id):
    """
    功能三：解析 PPT (图像) JSON 文件，并【多线程】执行【Aliyun OCR】。
    将其转换为“知识卡片” (用于ES)。
    """
    print(f"开始解析 PPT JSON -> 知识卡片 (多线程 Aliyun OCR): {ppt_json_path}")

    try:
        with open(ppt_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        ppt_data = data.get('PptExtraction', {})
        key_frames = ppt_data.get('KeyFrameList', [])

        if not key_frames:
            print("警告：PPT JSON 中未找到 'PptExtraction.KeyFrameList' 数据。")
            return []

        # --- 【V3 核心：多线程】 ---
        # 1. 准备任务列表
        # (我们把任务需要的所有参数打包成一个元组)
        tasks = []
        for i, frame in enumerate(key_frames):
            tasks.append((video_id, frame, i, len(key_frames)))

        print(f"  > 准备就绪：将使用多线程并发处理 {len(tasks)} 页PPT (Aliyun OCR)。")

        # 2. 创建线程池
        # max_workers=10 表示最多同时运行10个 API 请求
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            # 3. 使用 executor.map 来并发执行任务
            # (这里调用的是V3.2版的 _process_one_frame)
            results = list(executor.map(_process_one_frame, tasks))

        # 4. 过滤掉可能失败的任务 (例如返回None)
        knowledge_cards = [card for card in results if card is not None]
        # --- 【多线程结束】 ---

        print(f"\nPPT 解析完毕：生成 {len(knowledge_cards)} 张“知识卡片”。")
        return knowledge_cards

    except Exception as e:
        print(f"解析 PPT JSON 时发生未知错误: {e}")
        return []


# --- 4. 单元测试入口 (保持原样) ---
if __name__ == "__main__":

    print("--- [JSON Transformer 模块单元测试 (V3.2 - Aliyun OCR 版)] ---")

    # --- 【请修改这里用于测试】 ---
    RESULTS_DIR = "analysis_results"
    # TEST_TASK_ID = "22c4578e2d6449dc81b0a3f0f2ae8c5c"
    TEST_TASK_ID = "0cc6a679f34b6277"  # <-- 已按您上个请求修改
    # --- 【修改结束】 ---

    ASR_FILE_PATH = os.path.join(RESULTS_DIR, f"{TEST_TASK_ID}_ASR_Result.json")
    PPT_FILE_PATH = os.path.join(RESULTS_DIR, f"{TEST_TASK_ID}_PPT_Result.json")
    SRT_SAVE_PATH = os.path.join(RESULTS_DIR, f"{TEST_TASK_ID}_Subtitles.srt")

    # 1. 测试 SRT 生成
    print("\n[测试 1/3] 执行 SRT 字幕生成...")
    if generate_srt_file(ASR_FILE_PATH, SRT_SAVE_PATH):
        print(f" > SRT 文件已保存到: {SRT_SAVE_PATH}")
    else:
        print(" > SRT 文件生成失败。")

    # 2. 测试 ASR 解析
    print("\n[测试 2/3] 执行 ASR 卡片解析...")
    asr_cards = parse_asr_json_to_cards(ASR_FILE_PATH, TEST_TASK_ID)
    if asr_cards:
        print("\n  --- ASR 卡片示例 (第一张) ---")
        print(json.dumps(asr_cards[0], indent=2, ensure_ascii=False))
    else:
        print("  > ASR 解析未产生卡片。")

    # 3. 测试 PPT 解析 (现在会调用 Aliyun OCR)
    print("\n[测试 3/3] 执行 PPT 卡片解析 (多线程 Aliyun OCR)...")
    start_time = time.time()
    ppt_cards = parse_ppt_json_to_cards(PPT_FILE_PATH, TEST_TASK_ID)
    end_time = time.time()

    if ppt_cards:
        # 这个时间现在会快得多！
        print(f"\n  > 多线程 Aliyun OCR 处理完成，耗时: {end_time - start_time:.2f} 秒")
        print("\n  --- PPT 卡片示例 (第一张) ---")
        print(json.dumps(ppt_cards[0], indent=2, ensure_ascii=False))
    else:
        print("  > PPT 解析未产生卡片。")

    print(f"\n--- [JSON Transformer 模块单元测试 完成] ---")
    print(f"总计：生成 {len(asr_cards)} 张 ASR 卡片，{len(ppt_cards)} 张 PPT 卡片。")