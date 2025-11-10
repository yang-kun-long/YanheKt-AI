
-----

# 延河课堂“洞察”引擎

[](https://opensource.org/licenses/MIT)

一个由 AI 驱动的“延河课堂”视频索引与检索工具。它将“黑盒”般的视频课程“解锁”为可搜索、可跳转的知识库，提供“AI 助教”般的沉浸式学习体验。

-----

## 🚀 核心功能

> [\!NOTE]
> **解决了什么问题？**
> “延河课堂”的视频是“黑盒”，复习时无法检索，只能低效地拖动进度条。
>
> **“洞察”引擎的解决方案：**
> 本项目利用 AI 将视频的语音（ASR）和画面文字（OCR）“翻译”成带时间戳的知识索引，实现“**自然语言驱动的精准跳转**”。

`[此处应有一张 GIF/截图，展示在播放页搜索 "B+树" 并点击时间戳跳转的画面]`

  * **✨ AI 赋能**：自动调用“通义听悟”生成 **ASR 字幕**和**PPT 文字**索引。
  * **🔍 精准检索**：在播放页注入“AI 助教”搜索框，可检索视频内的全部语音和 PPT 内容。
  * **🚀 即时跳转**：点击搜索结果，视频播放器“空降”到对应的知识点时间戳。
  * **💡 状态同步**：前端按钮自动同步后端状态（“开始洞悉” -\> “洞悉完成”），防止重复操作。
  * **🧠 断点续传**：后端 AI 管线采用幂等设计，任务失败（如 ES 索引）可从失败点重试，无需重新上传。
  * **🔒 沉浸体验**：通过“油猴脚本”无缝嵌入 `yanhekt.cn`，无需离开原页面。
  * **📄 PPT 导出**：一键导出 AI 提取的 PPT 对应 PDF 文件。

## 🏛️ C-H-S 架构

本项目采用“C 端驱动的混合式 AI 工作流”架构，由三个核心组件协同工作：

1.  **[C] C-Proxy (视频获取插件)**

      * **形态**：油猴脚本 (SolidJS)
      * **页面**：`yanhekt.cn/course/*` (课程列表页)
      * **职责**：充当“C 端代理”。利用用户\*\*合法会E话（`localStorage`）**鉴权，实现视频流的**“下载-分片上传”\*\*管线。

2.  **[H] H-Host (后端原型机)**

      * **形态**：Flask API (`app.py`)
      * **页面**：`http://127.0.0.1:5000` (本地服务器)
      * **职责**：充当“大脑”。负责接收 C 端分片、合并视频、**编排 AI 管线**（OSS -\> Tingwu -\> OCR -\> ES）、管理**全局状态机** (`state_schema.json`)。

3.  **[S] S-Service (提问与字幕插件)**

      * **形态**：油猴脚本 (JS)
      * **页面**：`yanhekt.cn/session/*` (视频播放页)
      * **职责**：充当“AI 助教”。负责\*\*“消费”\*\* AI 成果，提供**字幕、搜索、跳转**功能。

## 💻 技术栈

| 类别 | 技术 | 职责 |
| :--- | :--- | :--- |
| **后端 (H-Host)** | Python, Flask | 轻量级 API 服务器 |
| | `state_schema.json` | 基于文件的轻量级**状态机** (实现幂等与断点续传) |
| **前端 (C-End)** | TypeScript, SolidJS, Vite | C-Proxy (下载器) UI 框架与构建 |
| | Tampermonkey (油猴) | 脚本运行环境 |
| **云服务**| 阿里云 OSS | 存储视频文件，作为 AI 服务的**内网**数据源 |
| | 阿里通义听悟 | **核心 AI 服务** (ASR, PPT 提取) |
| | 阿里云 OCR | **混合 AI** (辅助 `json_transformer` 深度识别 PPT) |
| | Elasticsearch | 存储 ASR 和 PPT 知识卡片，提供搜索 |

## 🚀 部署与使用

部署“洞察”引擎需要启动后端、配置依赖、安装前端三个步骤。

### 1\. 准备：依赖服务

确保 **Elasticsearch** 服务已启动，并准备好**阿里云**账号（具有 OSS, 通义听悟, 阿里云 OCR 权限）。

### 2\. 后端 (H-Host)：`app.py`

1.  **克隆/下载后端代码。**
2.  **安装依赖**：
    ```bash
    # 建议使用虚拟环境
    pip install Flask flask_cors aliyun-python-sdk-core oss2 elasticsearch requests python-dotenv
    # (可选) 用于视频转封装
    pip install imageio-ffmpeg 
    ```
3.  **配置环境变量**：
      * 在 `app.py` 同级目录创建 `.env` 文件。
      * 填入所有必需的凭证（参考 `config.py`）：
    <!-- end list -->
    ```.env
    # 阿里云 (通义听悟和 OSS 共用)
    OSS_TEST_ACCESS_KEY_ID=...
    OSS_TEST_ACCESS_KEY_SECRET=...
    TINGWU_ACCESS_KEY_ID=${OSS_TEST_ACCESS_KEY_ID}
    TINGWU_ACCESS_KEY_SECRET=${OSS_TEST_ACCESS_KEY_SECRET}
    TINGWU_APP_KEY=...

    # OSS 存储桶
    OSS_TEST_BUCKET=your-bucket-name
    OSS_TEST_ENDPOINT=https://oss-cn-beijing.aliyuncs.com
    OSS_TEST_ENDPOINT_INTERNAL=https://oss-cn-beijing-internal.aliyuncs.com

    # Elasticsearch
    ES_ENDPOINT=https://your-es-endpoint.com:9200
    ES_USERNAME=elastic
    ES_PASSWORD=...
    ```
4.  **启动后端**：
    ```bash
    python app.py
    # 看到 "Run: http://127.0.0.1:5000" 即表示成功
    ```

### 3\. 前端 (C-End)：油猴脚本

1.  **安装 Tampermonkey (油猴)** 浏览器插件。
2.  **构建脚本**：
      * `cd` 到前端 Vite 项目目录。
      * `npm install`
      * `npm run build`
      * 这将在 `dist`（或项目根）目录生成两个 `.user.js` 文件。
3.  **安装脚本**：
      * 打开 Tampermonkey 管理面板。
      * 将以下**两个**文件拖入面板进行安装，并**启用**它们：
        1.  `yanhekt-downloader.user.js` (C-Proxy 插件)
        2.  `yanhekt-player.user.js` (S-Service 插件)
      * *（如果前端 `endpoint` 地址不是 `127.0.0.1:5000`，请在构建前修改 `src/lib/ingestion.ts` 等文件中的地址）*

## 📖 使用流程

1.  **(列表页)** 登录“延河课堂”。`app.py` 终端应有 `precheck` 请求日志。课程列表项右侧出现“**开始洞悉**”按钮。
2.  点击“**开始洞D**” -\> 在弹窗中点击“**创建**”。
3.  屏幕右下角出现“抓取”进度条（`Snackbar`），开始执行**管线 1 (抓取-上传)**。
4.  （等待 `MERGING`, `TRANSCODING` 完成后）进度条变为“已完成”状态，并显示一个“**AI 洞悉**”按钮。
5.  点击“**AI 洞悉**”按钮，触发**管线 2 (后端 AI)**。
6.  `Snackbar` 状态变为 `(服务器处理中...)` -\> `(听悟解析中...)` -\> `(构建知识卡片...)`。
7.  （几分钟后）`Snackbar` 消失，且列表页上的“**开始洞悉**”按钮变为“**洞悉完成**”（灰色、禁用）。
8.  **(播放页)** 进入该视频的播放页。
9.  **享受功能**：
      * 视频底部自动显示 **AI 字幕**。
      * 右下角出现“**AI 助教**”悬浮框。
      * 在框中**提问**（如“B+树”），即可搜索，并**点击时间戳**跳转。
      * 点击“**导出 PPT**”按钮。


## 📄 授权 (License)

[MIT License](https://github.com/yang-kun-long/YanheKt-AI?tab=MIT-1-ov-file)
