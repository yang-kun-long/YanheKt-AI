// src/components/Snackbar.tsx
import {
  type Component,
  For,
  Match,
  ParentComponent,
  Switch,
  createSignal,
  onCleanup,
  onMount,
} from "solid-js"
import { CourseVideo, downloadVideo, fetchCourseVideos } from "../lib/course"
import downloadingSVG from "@assets/icons/downloading.svg"
import errorSVG from "@assets/icons/error.svg"
import processingSVG from "@assets/icons/processing.svg"
import waitingSVG from "@assets/icons/waiting.svg"
import finishedSVG from "@assets/icons/finished.svg"
// ❌ 不再用云下载图标
// import cloudDownloadSVG from "@assets/icons/cloud-download.svg"
import cancelSVG from "@assets/icons/cancel.svg"
import retrySVG from "@assets/icons/retry.svg"
import { createListTransition } from "@solid-primitives/transition-group"
import { resolveElements } from "@solid-primitives/refs"
import { preflightIngestion, startIngestion } from "../lib/ingestion"

export interface DownloadTask {
  courseId: string
  courseName: string
  courseTitle: string
  videoType: "vga" | "main" // 会被忽略，统一按 vga 下载
  autoTranscode: boolean
}

let courseVideos: CourseVideo[] // lazy load
let metaForId: { videoId: number; sessionId: number;startedAt: string } | undefined
const FlexColumnGap = 12

export const TaskSnackbarHost: Component<{
  tasks: DownloadTask[]
  onComplete: (index: number) => void
  onRetry: (index: number) => void
  // 1. 接收来自 App.tsx 的 onStageChange prop
  onStageChange: (index: number, stage: string) => void
}> = (props) => {
  return (
    <div
      style={{ gap: `${FlexColumnGap}px` }}
      class="fixed bottom-[24px] right-[24px] flex flex-col-reverse"
    >
      <TransitionGroup>
        <For each={props.tasks}>
          {(task, index) => (
            <TaskSnackbar
              task={task}
              onComplete={() => props.onComplete(index())}
              onRetry={() => props.onRetry(index())}
              // 2. 将 stage 变化传递给子组件，并绑定 index
              onStageChange={(stage) => props.onStageChange(index(), stage)}
            />
          )}
        </For>
      </TransitionGroup>
    </div>
  )
}

// ... TransitionGroup (无变化) ...
const TransitionGroup: ParentComponent = (props) => {
  const transition = createListTransition(
    /* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call,
    @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-member-access */
    resolveElements(() => props.children).toArray,
    /* eslint-enable */
    {
      exitMethod: "keep-index",
      onChange({ added, removed, finishRemoved }) {
        ;(added as HTMLElement[]).forEach((el) => {
          el.style.opacity = "0"
          el.style.transform = "translateY(-24px)"
          setTimeout(() => {
            el.style.transition = "all 0.4s ease-out"
            el.style.opacity = "1"
            el.style.transform = "none"
          })
        })
        ;(removed as HTMLElement[]).forEach((el) => {
          el.style.transition = "all 0.3s ease-out"
          el.style.opacity = "0"
          el.style.transform = "translateX(-24px)"
          el.style.marginTop = `-${el.clientHeight + FlexColumnGap}px`
          el.addEventListener(
            "transitionend",
            () => finishRemoved([el]),
            { once: true },
          )
        })
      },
    },
  )
  return <>{transition()}</>
}


// ... TaskSnackbarState (无变化) ...
type TaskSnackbarState =
  | TaskSnackbarState.Waiting
  | TaskSnackbarState.Downloading
  | TaskSnackbarState.Uploading
  | TaskSnackbarState.Transcoding
  | TaskSnackbarState.Finished
  | TaskSnackbarState.Error
namespace TaskSnackbarState {
  export interface Waiting {
    status: "waiting"
    // 增加 'check'（预检/查重）和 'server'（后端排队或洞悉流水线）
    type: "check" | "download" | "upload" | "transcode" | "server"
    label?: string
  }
  export interface Downloading {
    status: "downloading"
    progress: number // 0..1
  }
  export interface Uploading {
    status: "uploading"
    progress: number // 0..1
  }
  export interface Transcoding {
    status: "transcoding"
    progress: number // 0..1
  }
  export interface Finished {
    status: "finished"
    blobUrl: string // 这里保存后端成品的下载链接（用于解析出 objectId）
  }
  export interface Error {
    status: "error"
    type: "download" | "upload" | "transcode" | "cancelled"
    message: string
  }
}


export const TaskSnackbar: Component<{
  task: DownloadTask
  onComplete: () => void
  onRetry: () => void
  // 3. 接收来自 TaskSnackbarHost 的 onStageChange prop
  onStageChange: (stage: string) => void
}> = (props) => {
  const [state, setState] = createSignal<TaskSnackbarState>({
    status: "waiting",
    type: "check",
  })
  let abortDownload: (() => void) | undefined
  let abortUploading: (() => void) | undefined

  const cancel = () => {
    abortDownload?.()
    abortUploading?.()
    props.onComplete()
  }

  void (async function run() {
    try {
      const url = await resolveVideoUrlVGAOnly()

      // 预检（查重/幂等）
      setState({ status: "waiting", type: "check" })
      const pre = await preflightIngestion(
        {
          courseId: props.task.courseId,
          courseName: props.task.courseName,
          courseTitle: props.task.courseTitle,
          source: "projector",
          autoTranscode: true,
          originalFilename: `[${props.task.courseName}] ${props.task.courseTitle} [投影].ts`,
          videoId: metaForId?.videoId,
          sessionId: metaForId?.sessionId,
          startedAt: metaForId?.startedAt,
          videoType: "vga",
        },
        {
          endpoint: "http://127.0.0.1:5000",
          onServerStage: (stage) => {
            // 4. (位置 1/3) 上报 preflight 的 stage
            props.onStageChange(stage)

            if (stage === "EXISTS") {
              setState({ status: "waiting", type: "check", label: "已存在，直接可洞悉" })
            } else if (stage === "NOT_EXISTS") {
              setState({ status: "waiting", type: "check", label: "未存在，准备下载" })
            }
          },
        },
      )

      if (pre.exists) {
        const link = pre.downloadUrl ?? pre.rawUrl ?? ""
        if (!link) throw new Error("server returned exists but no url")
        // 直接进入“已完成”，此时按钮为“AI洞悉”
        setState({ status: "finished", blobUrl: link })
        return
      }

      // 下载 TS
      const tsBlob = await startDownloading(url)

      // 上传 → 合并/转封装 → 完成
      if (props.task.autoTranscode) {
        await startServerPipeline(tsBlob, pre)
      } else {
        setState({
          status: "finished",
          blobUrl: URL.createObjectURL(tsBlob),
        })
      }
    } catch {
      /* 各自设置错误状态 */
    }

    // ... resolveVideoUrlVGAOnly (无变化) ...
    async function resolveVideoUrlVGAOnly() {
      if (!courseVideos) {
        try {
          courseVideos = await fetchCourseVideos(props.task.courseId)
        } catch (err) {
          console.log("%c[Downloading]", "color: red", err)
          setState({
            status: "error",
            type: "download",
            message: "获取课程视频地址失败",
          })
        }
      }
      const match = courseVideos.find(
        (course) => course.title === props.task.courseTitle,
      )
      if (!match) {
        setState({
          status: "error",
          type: "download",
          message: "未找到匹配的课程节次",
        })
        throw new Error("course session not found")
      }
      const videoDetail = match.videos?.[0]
      if (!videoDetail || videoDetail.format !== "m3u8") {
        setState({
          status: "error",
          type: "download",
          message: "不支持下载当前课程视频",
        })
        throw new Error("unsupported format")
      }
      const url = videoDetail.vga
      if (!url) {
        setState({
          status: "error",
          type: "download",
          message: "未提供 VGA 源",
        })
        throw new Error("vga url missing")
      }
      metaForId = { 
        videoId: videoDetail.id, 
        sessionId: match.id, 
        startedAt: match.started_at 
      }
      return url
    }

    // ... startDownloading (无变化) ...
    async function startDownloading(url: string) {
      try {
        setState({ status: "waiting", type: "download" })
        const [tsBlob, cancel] = downloadVideo(url, (progress) => {
          setState({ status: "downloading", progress })
        })
        abortDownload = cancel
        return await tsBlob
      } catch (error) {
        console.log("%c[Downloading]", "color: red", error)
        if (error instanceof DOMException && error.name === "AbortError") {
          setState({
            status: "error",
            type: "cancelled",
            message: "下载已取消",
          })
        } else {
          setState({
            status: "error",
            type: "download",
            message: "下载错误",
          })
        }
        throw error
      }
    }


    async function startServerPipeline(tsBlob: Blob, preflight?: { exists: false; uploadId?: string }) {
      try {
        setState({ status: "waiting", type: "upload" })
        setState({ status: "uploading", progress: 0 })

        const finalName = `[${props.task.courseName}] ${props.task.courseTitle} [投影].mp4`

        const [promise, cancel] = startIngestion(
          tsBlob,
          {
            courseId: props.task.courseId,
            courseName: props.task.courseName,
            courseTitle: props.task.courseTitle,
            source: "projector",
            autoTranscode: true,
            originalFilename: finalName.replace(/\.mp4$/, ".ts"),
            videoId: metaForId?.videoId,
            startedAt: metaForId?.startedAt,
            videoType: "vga",
          },
          {
            endpoint: "http://127.0.0.1:5000",
            partSize: 8 * 1024 * 1024,
            concurrency: 4,
            preflightResult: preflight as any,
            onUploadProgress: (p) =>
              setState({ status: "uploading", progress: p }),
            onServerStage: (stage, prog) => {
              // 5. (位置 2/3) 上报 ingestion 的 stage
              props.onStageChange(stage)
              
              if (stage === "QUEUED") {
                setState({ status: "waiting", type: "server", label: "排队中" })
              } else if (stage === "MERGING" || stage === "TRANSCODING") {
                setState({ status: "transcoding", progress: prog })
              } else if (stage === "DONE") {
                setState({ status: "transcoding", progress: 1 })
              }
            },
          },
        )
        abortUploading = cancel
        const res = await promise
        const link = res.downloadUrl ?? ""
        setState({ status: "finished", blobUrl: link })
      } catch (error) {
        console.log("%c[ServerPipeline]", "color: red", error)
        if (error instanceof DOMException && error.name === "AbortError") {
          setState({
            status: "error",
            type: "cancelled",
            message: "已取消",
          })
        } else {
          setState({
            status: "error",
            type: "transcode",
            message: "服务器处理失败",
          })
        }
        throw error
      }
    }
  })()

  // ====== 新增：洞悉流水线 ======
  function extractObjectIdFromUrl(url: string): string | undefined {
    // 兼容: /api/download/<objectId>(?raw=ts)
    const m = url.match(/\/api\/download\/([a-f0-9]{16})/i)
    return m?.[1]
  }

  async function startInsightFlow() {
    try {
      const url = (state() as TaskSnackbarState.Finished).blobUrl
      const objectId = extractObjectIdFromUrl(url)
      if (!objectId) {
        setState({
          status: "error",
          type: "transcode",
          message: "未能解析视频标识（objectId）",
        })
        return
      }

      // 启动洞悉
      setState({ status: "waiting", type: "server", label: "启动洞悉..." })
      const createRes = await fetch(`http://127.0.0.1:5000/api/insights`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ objectId }),
      })
      if (!createRes.ok) throw new Error(`insight create http ${createRes.status}`)

      // 轮询洞悉状态
      let done = false
      while (!done) {
        const st = await fetch(`http://127.0.0.1:5000/api/insights/${objectId}/status`)
        if (!st.ok) throw new Error(`insight status http ${st.status}`)
        const data = await st.json()
        const stage = (data.stage as string) || "UNKNOWN"
        const prog = Number(data.progress ?? 0)

        // 6. (位置 3/3) 上报 insight 轮询的 stage
        props.onStageChange(stage)

        // 映射阶段 → 文案
        if (stage === "CHECK") {
          setState({ status: "waiting", type: "server", label: "检查历史记录..." })
        } else if (stage === "OSS_UPLOAD") {
          setState({ status: "waiting", type: "server", label: "上传 OSS..." })
        } else if (stage === "OSS_CLEAN") {
          setState({ status: "waiting", type: "server", label: "清理 OSS..." })
        } else if (stage === "AI_SUBMIT") {
          setState({ status: "waiting", type: "server", label: "提交听悟..." })
        } else if (stage === "AI_POLL") {
          setState({ status: "waiting", type: "server", label: `听悟解析中（${Math.round(prog * 100)}%）...` })
        } else if (stage === "ES_INDEX") {
          setState({ status: "waiting", type: "server", label: "构建知识卡片/入索引..." })
        } else if (stage === "DONE") {
          setState({ status: "finished", blobUrl: url }) // 重回完成态（按钮仍在，可再次洞悉）
          done = true
          break
        } else if (stage === "FAILED" || stage === "UNKNOWN") {
          setState({
            status: "error",
            type: "transcode",
            message: data.message || "洞悉失败",
          })
          return
        } else {
          setState({ status: "waiting", type: "server", label: "服务器处理中..." })
        }

        await new Promise((r) => setTimeout(r, 1000))
      }
    } catch (e) {
      console.log("%c[InsightPipeline]", "color: red", e)
      setState({
        status: "error",
        type: "transcode",
        message: "服务器处理失败",
      })
    }
  }
  
  // ... prettyWaitingText (无变化) ...
  const prettyWaitingText = (s: TaskSnackbarState.Waiting) => {
    if (s.type === "check") return s.label ?? "检查是否已存在..."
    if (s.type === "download") return "等待下载..."
    if (s.type === "upload") return "等待上传..."
    if (s.type === "transcode") return "等待转码..."
    if (s.type === "server") return s.label ? `服务器处理中（${s.label}）...` : "服务器处理中..."
    return "处理中..."
  }

  // ... return (...) (无变化) ...
  return (
    <div class="snackbar">
      <Switch>
        <Match when={state().status == "waiting"}>
          <SnackbarScaffold
            iconUrl={waitingSVG}
            title={props.task.courseTitle}
            description={prettyWaitingText(state() as TaskSnackbarState.Waiting)}
            color="gray"
            progress={0}
            actionIconUrl={cancelSVG}
            onActionClick={cancel}
          />
        </Match>

        <Match when={state().status == "downloading"}>
          <SnackbarScaffold
            iconUrl={downloadingSVG}
            title={props.task.courseTitle}
            description="下载中..."
            color="#699AE4"
            progress={(state() as TaskSnackbarState.Downloading).progress}
            actionIconUrl={cancelSVG}
            onActionClick={cancel}
          />
        </Match>

        <Match when={state().status == "uploading"}>
          <SnackbarScaffold
            iconUrl={downloadingSVG}
            title={props.task.courseTitle}
            description="上传中..."
            color="#7C9AEC"
            progress={(state() as TaskSnackbarState.Uploading).progress}
            actionIconUrl={cancelSVG}
            onActionClick={cancel}
          />
        </Match>

        <Match when={state().status == "transcoding"}>
          <SnackbarScaffold
            iconUrl={processingSVG}
            title={props.task.courseTitle}
            description="服务器处理中（合并/转封装/洞悉）..."
            color="#69E4DD"
            progress={(state() as TaskSnackbarState.Transcoding).progress}
            actionIconUrl={cancelSVG}
            onActionClick={cancel}
          />
        </Match>

        <Match when={state().status == "finished"}>
          {/* 改为“AI洞悉” */}
          <div class="relative h-[64px] w-[288px] flex items-center justify-between gap-[12px] overflow-hidden rounded-[6px] bg-white px-[12px] py-[16px] shadow-md">
            <img src={finishedSVG} alt="" class="h-full" />
            <div class="flex flex-1 flex-col justify-between overflow-hidden">
              <div class="w-full overflow-hidden text-ellipsis break-keep text-[0.9em] text-[#333]">
                {props.task.courseTitle}
              </div>
              <div class="select-none text-[0.8em] opacity-75">
                已完成（可发起 AI 洞悉）
              </div>
            </div>
            <button
              type="button"
              class="h-3/5 cursor-pointer rounded-[4px] border border-[#0f86ff] bg-white px-3 text-[#0f86ff] text-sm hover:bg-[#eaf4ff] active:brightness-95"
              onClick={startInsightFlow}
            >
              AI洞S
            </button>
            <div
              style={{ width: `100%`, background: "#69E48B" }}
              class="absolute bottom-0 left-0 h-[3px] rounded-[2px]"
            />
          </div>
        </Match>

        <Match when={state().status == "error"}>
          <SnackbarScaffold
            iconUrl={errorSVG}
            title={props.task.courseTitle}
            description={(state() as TaskSnackbarState.Error).message}
            color="#E46969"
            progress={1}
            actionIconUrl={retrySVG}
            onActionClick={props.onRetry}
          />
        </Match>
      </Switch>
    </div>
  )
}

// ... SnackbarScaffold (无变化) ...
const SnackbarScaffold: Component<{
  iconUrl: string
  title: string
  description: string
  color: string
  progress: number
  actionIconUrl: string
  onActionClick: () => void
}> = (props) => {
  const [fade, setFade] = createSignal(true)
  onMount(() => {
    setTimeout(() => setFade(false))
  })
  return (
    <div class="relative h-[64px] w-[288px] flex items-center justify-between gap-[12px] overflow-hidden rounded-[6px] bg-white px-[12px] py-[16px] shadow-md">
      <img
        classList={{ "opacity-0": fade() }}
        src={props.iconUrl}
        alt=""
        class="h-full transition-opacity duration-300"
      />
      <div class="flex flex-1 flex-col justify-between overflow-hidden">
        <div class="w-full overflow-hidden text-ellipsis break-keep text-[0.9em] text-[#333]">
          {props.title}
        </div>
        <div class="select-none text-[0.8em] opacity-75">
          {props.description}
        </div>
      </div>
      <div class="h-full flex items-center">
        <ActionButton
          onClick={props.onActionClick}
          class="h-3/5 cursor-pointer border-none bg透明 p-0 active:brightness-90 hover:brightness-110"
        >
          <img src={props.actionIconUrl} alt="" class="h-full" />
        </ActionButton>
      </div>
      <div
        style={{ width: `${props.progress * 100}%`, background: props.color }}
        class="absolute bottom-0 left-0 h-[3px] rounded-[2px]"
      />
    </div>
  )
}

// ... ActionButton (无变化) ...
const ActionButton: ParentComponent<{
  class: string
  onClick: () => void
}> = (props) => {
  return (
    <button
      type="button"
      class={props.class}
      onClick={() => props.onClick()}
    >
      {props.children}
    </button>
  )
}