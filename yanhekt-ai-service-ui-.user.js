// ==UserScript==
// @name          延河课堂洞察引擎C端服务
// @namespace    http://tampermonkey.net/
// @version      1.9.1 
// @description  延河课堂洞察引擎的C 端服务界面，作为“AI 助教”运行在课程播放页。它是系统价值的最终呈现者，负责**“消费-检索-跳转”**管线。
// @author       杨昆龙
// @match        https://www.yanhekt.cn/session/*
// @connect      127.0.0.1
// @grant        GM_addStyle
// @grant        GM_xmlhttpRequest
// ==/UserScript==

(function() {
    'use strict';

    // --- 配置 ---
    const BACKEND_URL = "http://127.0.0.1:5000"; // 你的本地后端地址

    // --- 状态变量 ---
    let currentSessionId = null;
    let currentObjectId = null;
    let subtitles = [];

    let mainVideoElement = null;
    let vgaVideoElement = null; // <-- 正确声明 (大写 V)

    // --- UI 元素 ---
    let subtitleBox = null;
    let searchInput = null;
    let searchResultsBox = null;

   /**
    * 1. 注入 CSS 样式 (与 1.9.1 相同)
    */
    function injectStyles() {
        GM_addStyle(`
            /* --- 1. 容器：(V1.9 - 可拖动/缩放) --- */
            #ai-qa-container {
                position: fixed;
                bottom: 20px;
                right: 20px;
                width: 50vw;
                min-width: 400px;
                max-width: 1200px;
                height: 450px;
                min-height: 200px;
                max-height: 90vh;
                resize: both;
                overflow: auto;
                background: #ffffff; border: 1px solid #e0e0e0;
                border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                z-index: 9999; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', sans-serif;
                display: flex; flex-direction: column;
            }
            #ai-qa-header {
                padding: 10px 15px;
                font-weight: 600;
                color: #333;
                border-bottom: 1px solid #f0f0f0;
                cursor: move;
                flex-shrink: 0;
                user-select: none;
            }
            #ai-qa-search-box {
                display: flex;
                padding: 10px;
                border-bottom: 1px solid #f0f0f0;
                flex-shrink: 0;
            }

            /* --- V1.9.1 样式 --- */
            #ai-qa-search-box input {
                flex-grow: 1;
                border: 1px solid #d9d9d9;
                border-radius: 4px;
                padding: 5px 8px;
                font-size: 14px;
            }
            #ai-qa-search-box button {
                margin-left: 8px;
                padding: 5px 12px;
                border: none;
                background-color: #0f86ff;
                color: white;
                border-radius: 4px;
                cursor: pointer;
            }
            #ai-qa-search-box button:hover {
                background-color: #0073e6;
            }
            #ai-qa-export-ppt {
                margin-left: 8px;
                padding: 5px 12px;
                border: none;
                background-color: #28a745;
                color: white;
                border-radius: 4px;
                cursor: pointer;
            }
            #ai-qa-export-ppt:hover {
                background-color: #218838;
            }
            #ai-qa-export-ppt:disabled {
                background-color: #cccccc;
                cursor: not-allowed;
            }

            /* --- 2. 结果滚动区 --- */
            #ai-qa-results {
                padding: 10px 15px;
                overflow-y: auto;
                flex-grow: 1;
                min-height: 50px;
                font-size: 13px;
                line-height: 1.6;
            }
            #ai-qa-results .result-item { margin-bottom: 12px; border-bottom: 1px dashed #eee; padding-bottom: 8px; }
            #ai-qa-results .result-time { font-weight: 600; color: #0f86ff; cursor: pointer; }
            #ai-qa-results .result-text {
                color: #555;
                display: block !important;
                white-space: pre-wrap !important;
                overflow: visible !important;
                text-overflow: clip !important;
                word-break: break-word;
                height: auto !important;
                max-height: none !important;
            }

            /* 字幕框样式 (保持不变) */
            #ai-subtitle-box {
                position: absolute; bottom: 80px; left: 50%;
                transform: translateX(-50%);
                background: rgba(0, 0, 0, 0.7); color: white;
                padding: 8px 15px; border-radius: 6px;
                font-size: 18px; z-index: 9998;
                max-width: 80%; text-align: center;
                pointer-events: none;
                transition: opacity 0.2s ease-out;
                white-space: pre-wrap;
            }
        `);
    }

   /**
    * 2. 注入 HTML 框架 (与 1.9.1 相同)
    */
    function injectUI() {
        const container = document.createElement('div');
        container.id = "ai-qa-container";

        container.innerHTML = `
            <div id="ai-qa-header">AI 助教提问</div>
            <div id="ai-qa-search-box">
                <input type="text" id="ai-qa-input" placeholder="输入问题...">
                <button id="ai-qa-submit">搜索</button>
                <button id="ai-qa-export-ppt" disabled>导出 PPT</button>
            </div>
            <div id="ai-qa-results">
                请提问...
            </div>
        `;
        document.body.appendChild(container);

        subtitleBox = document.createElement('div');
        subtitleBox.id = "ai-subtitle-box";
        subtitleBox.style.display = 'none';

        // 绑定事件
        searchInput = document.getElementById('ai-qa-input');
        searchResultsBox = document.getElementById('ai-qa-results');
        const header = document.getElementById('ai-qa-header');
        document.getElementById('ai-qa-submit').addEventListener('click', handleQuestionSubmit);
        document.getElementById('ai-qa-export-ppt').addEventListener('click', handlePptExport);

        // --- 启用拖动 ---
        enableDraggable(container, header);
    }

    /**
     * V1.9: 使元素可拖动 (与 1.9.1 相同)
     */
    function enableDraggable(container, handle) {
        let offsetX, offsetY;
        handle.addEventListener('mousedown', (e) => {
            if (e.target.id !== 'ai-qa-header') { return; }
            e.preventDefault();
            const rect = container.getBoundingClientRect();
            offsetX = e.clientX - rect.left;
            offsetY = e.clientY - rect.top;
            if (container.style.right || container.style.bottom) {
                 container.style.left = rect.left + 'px';
                 container.style.top = rect.top + 'px';
                 container.style.right = 'auto';
                 container.style.bottom = 'auto';
            }
            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp, { once: true });
        });
        function onMouseMove(e) {
            let newX = e.clientX - offsetX;
            let newY = e.clientY - offsetY;
            const vpWidth = window.innerWidth;
            const vpHeight = window.innerHeight;
            const rect = container.getBoundingClientRect();
            if (newX < 0) newX = 0;
            if (newY < 0) newY = 0;
            if (newX + rect.width > vpWidth) newX = vpWidth - rect.width;
            if (newY + rect.height > vpHeight) newY = vpHeight - rect.height;
            container.style.left = newX + 'px';
            container.style.top = newY + 'px';
        }
        function onMouseUp() {
            document.removeEventListener('mousemove', onMouseMove);
        }
    }


    /**
     * SRT 解析函数 (保持不变)
     */
    function parseSRT(srtText) {
        const subs = [];
        const lines = srtText.split(/\r?\n/);
        function timeToSeconds(timeStr) {
            const parts = timeStr.split(/[:,]/);
            return parseFloat(parts[0]) * 3600 + parseFloat(parts[1]) * 60 + parseFloat(parts[2]) + parseFloat(parts[3]) / 1000;
        }
        let i = 0;
        while (i < lines.length) {
            if (lines[i].match(/^\d+$/)) { i++; if (i >= lines.length) break; const timeLine = lines[i]; const timeMatch = timeLine.match(/(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})/);
                if (timeMatch) { i++; let text = ""; while (i < lines.length && lines[i].trim() !== "") { text += lines[i] + "\n"; i++; }
                    subs.push({ start: timeToSeconds(timeMatch[1]), end: timeToSeconds(timeMatch[2]), text: text.trim() });
                }
            } i++;
        } return subs;
    }

    /**
     * 3. 页面加载后的主逻辑 (保持不变)
     */
    async function initialize() {
        console.log("AI 助教脚本启动...");
        injectStyles();
        injectUI();

        const match = window.location.href.match(/session\/(\d+)/);
        if (!match || !match[1]) { console.error("未能从 URL 中解析 Session ID"); return; }
        currentSessionId = match[1];
        console.log("当前 Session ID:", currentSessionId);

        try {
            console.log(`正在向后端查询 ${currentSessionId} 对应的 ObjectId...`);
            const objectIdResponse = await gmFetch_JSON(`${BACKEND_URL}/api/resolve_session/${currentSessionId}`);

            if (!objectIdResponse.ok) {
                throw new Error("后端解析 SessionId 失败: " + objectIdResponse.error);
            }
            currentObjectId = objectIdResponse.objectId;
            console.log(`成功获取 ObjectId: ${currentObjectId}`);

            // 启用 PPT 按钮
            const pptButton = document.getElementById('ai-qa-export-ppt');
            if (pptButton) {
                pptButton.disabled = false; // 启用按钮
            }

            console.log(`正在获取 ${currentObjectId} 的 SRT 字幕文件...`);
            const srtText = await gmFetch_Text(`${BACKEND_URL}/api/subtitles/${currentObjectId}`);
            subtitles = parseSRT(srtText);
            console.log(`成功加载 ${subtitles.length} 条字幕。`);

            findVideoPlayers();

        } catch (error) {
            console.error("AI 助教初始化失败:", error);
            if (searchResultsBox) {
                searchResultsBox.innerHTML = `<div style="color: red;">${error.message}</div>`;
            }
            const pptButton = document.getElementById('ai-qa-export-ppt');
            if (pptButton && !currentObjectId) {
                 pptButton.disabled = true;
            }
        }
    }

    /**
     * 4. 查找播放器并同步字幕 (*** V1.9.2 修正 ***)
     */
    function findVideoPlayers(retries = 0) {
        const mainPlayerContainer = document.getElementById("video_id_mainPlayer");
        const vgaPlayerContainer = document.getElementById("video_id_topPlayer");
        if (!mainPlayerContainer || !vgaPlayerContainer) {
            if (retries < 10) {
                console.warn("未找到播放器容器 (main/top)，1秒后重试...");
                setTimeout(() => findVideoPlayers(retries + 1), 1000);
            } else {
                console.error("重试 10 次后仍未找到播放器容器。");
            }
            return;
        }
        mainVideoElement = mainPlayerContainer.querySelector("video");

        // --- +++ V1.9.2 修正 +++ ---
        vgaVideoElement = vgaPlayerContainer.querySelector("video"); // <-- 修正了拼写 (V)
        // --- +++ 修正结束 +++ ---

        if (mainVideoElement && vgaVideoElement) { // <-- 现在这里能正确判断了
            console.log("✅ 已成功锁定 Main 播放器:", mainVideoElement);
            console.log("✅ 已成功锁定 VGA 播放器:", vgaVideoElement);
            vgaPlayerContainer.style.position = 'relative';
            vgaPlayerContainer.appendChild(subtitleBox);
            subtitleBox.style.display = 'block';
            vgaVideoElement.addEventListener('timeupdate', handleSubtitleSync);
            mainVideoElement.addEventListener('timeupdate', handleSubtitleSync);
            console.log("字幕框已注入 VGA 播放器，并已绑定两个播放器的 timeupdate 事件。");
        } else {
            console.error("找到了播放器容器，但未能在内部找到 <video> 元素。");
        }
    }

    /**
     * handleSubtitleSync (保持不变)
     */
    function handleSubtitleSync() {
        if (subtitles.length === 0) return;
        let currentTime = 0;
        if (vgaVideoElement && vgaVideoElement.offsetParent !== null) {
            currentTime = vgaVideoElement.currentTime;
        } else if (mainVideoElement) {
            currentTime = mainVideoElement.currentTime;
        } else {
            return;
        }
        const currentSubtitle = subtitles.find(sub =>
            currentTime >= sub.start && currentTime <= sub.end
        );
        let activePlayerContainer = null;
        if (vgaVideoElement && vgaVideoElement.offsetParent !== null) {
             activePlayerContainer = vgaVideoElement.parentElement;
        } else if (mainVideoElement) {
             activePlayerContainer = mainVideoElement.parentElement;
        }
        if (activePlayerContainer && subtitleBox.parentElement !== activePlayerContainer) {
            console.log("检测到播放器切换，正在移动字幕框...");
            activePlayerContainer.style.position = 'relative';
            activePlayerContainer.appendChild(subtitleBox);
        }
        if (currentSubtitle) {
            subtitleBox.textContent = currentSubtitle.text;
            subtitleBox.style.opacity = '1';
        } else {
            subtitleBox.style.opacity = '0';
        }
    }


    /**
     * 5. 提交问题 (搜索) - (保持不变)
     */
    async function handleQuestionSubmit() {
        const query = searchInput.value;
        if (!query) return;
        if (!currentObjectId) {
            searchResultsBox.innerHTML = '<div style="color: red;">错误：未初始化 ObjectId，无法搜索。</div>';
            return;
        }
        console.log(`搜索: ${query}, ObjectId: ${currentObjectId}`);
        searchResultsBox.innerHTML = '正在搜索...';
        try {
            const searchUrl = `${BACKEND_URL}/api/search?q=${encodeURIComponent(query)}&videoId=${currentObjectId}`;
            const results = await gmFetch_JSON(searchUrl);
            console.log("!!! [Search] 收到来自后端的完整 JSON:", results);
            if (!results.hits || results.hits.length === 0) {
                searchResultsBox.innerHTML = '未找到相关结果。';
                return;
            }
            searchResultsBox.innerHTML = '';
            results.hits.forEach(hit => {
                const resultEl = document.createElement('div');
                resultEl.className = 'result-item';
                const timeInSeconds = hit.start_ms / 1000.0;
                const timeString = `[${hit.time_str}]`;
                resultEl.innerHTML = `
                    <div class="result-time" data-time="${timeInSeconds}">${timeString}</div>
                    <div class="result-text">${hit.content}</div>
                `;
                searchResultsBox.appendChild(resultEl);
            });
            searchResultsBox.querySelectorAll('.result-time').forEach(el => {
                el.addEventListener('click', () => {
                    const time = parseFloat(el.getAttribute('data-time'));
                    if (mainVideoElement) {
                        mainVideoElement.currentTime = time;
                        mainVideoElement.play();
                    }
                    if (vgaVideoElement) {
                        vgaVideoElement.currentTime = time;
                        vgaVideoElement.play();
                    }
                });
            });
        } catch (error) {
            console.error("搜索失败:", error);
            searchResultsBox.innerHTML = `<div style="color: red;">搜索失败: ${error.message}</div>`;
        }
    }

    /**
     * 6. 处理 PPT 导出点击 (保持不变)
     */
    function handlePptExport() {
        if (currentObjectId) {
            // 直接构建后端重定向 URL
            const exportUrl = `${BACKEND_URL}/api/insights/${currentObjectId}/ppt`;
            console.log("正在打开 PPT 导出 URL:", exportUrl);
            window.open(exportUrl, '_blank');
        } else {
            console.warn("PPT 导出按钮被点击，但 currentObjectId 为空。");
            alert("错误：ObjectId 尚未加载，无法导出。");
        }
    }

    /**
     * 7. 辅助函数：GM_xmlhttpRequest (保持不变)
     */
    function gmFetch_JSON(url) {
        const cacheBustUrl = new URL(url);
        cacheBustUrl.searchParams.set('_cache_bust', new Date().getTime());
        return new Promise((resolve, reject) => {
            GM_xmlhttpRequest({
                method: "GET",
                url: cacheBustUrl.toString(),
                onload: function(response) {
                    if (response.status >= 200 && response.status < 300) {
                        try { resolve(JSON.parse(response.responseText)); }
                        catch (e) { reject(new Error("JSON 解析失败")); }
                    } else {
                        try {
                            const err = JSON.parse(response.responseText);
                            reject(new Error(err.error || `HTTP ${response.status}`));
                        } catch(e) { reject(new Error(`HTTP ${response.status} ${response.statusText}`)); }
                    }
                },
                onerror: function(error) { reject(new Error("网络请求失败 (请检查本地后端是否已启动并允许跨域)")); }
            });
        });
    }

    /**
     * 8. 辅助函数：GM_xmlhttpRequest (保持不变)
     */
    function gmFetch_Text(url) {
        const cacheBustUrl = new URL(url);
        cacheBustUrl.searchParams.set('_cache_bust', new Date().getTime());
        return new Promise((resolve, reject) => {
            GM_xmlhttpRequest({
                method: "GET",
                url: cacheBustUrl.toString(),
                onload: function(response) {
                    if (response.status >= 200 && response.status < 300) {
                        resolve(response.responseText);
                    } else {
                        try {
                            const err = JSON.parse(response.responseText);
                            reject(new Error(err.error || `HTTP ${response.status}`));
                        } catch(e) { reject(new Error(`HTTP ${response.status} ${response.statusText}`)); }
                    }
                },
                onerror: function(error) { reject(new Error("网络请求失败 (请检查本地后端是否已启动并允许跨域)")); }
            });
        });
    }

    // --- 启动！ ---
    if (document.readyState === 'complete') {
        initialize();
    } else {
        window.addEventListener('load', initialize);
    }

})();