// src/lib/ingestion.ts
// 功能：分片上传 + 预检（查重/幂等）+ 服务器状态透传
// - 新增 preflightIngestion(): 在下载 TS 之前请求后端，若已存在则直接返回下载链接并跳过整个上传流程
// - startIngestion(): 兼容旧用法（下载完再上传）；也支持复用 preflight 的 uploadId
// - onServerStage(): 透传后端 stage（QUEUED/MERGING/TRANSCODING/DONE/EXISTS/...）

export type IngestionMeta = {
  courseId: string
  courseName: string
  courseTitle: string
  source: "projector" | "classroom"
  total: number                 // 分片数（由前端计算；若走 preflight 可先不填，或填 0）
  autoTranscode: boolean
  originalFilename?: string

  // 用于后端统一命名 / 幂等去重（建议传）
  videoId?: number
  sessionId?: number
  startedAt?: string
  videoType?: "vga" | "main"    // 你现在固定 "vga"
}

type Opts = {
  endpoint?: string                         // 默认 http://127.0.0.1:5000
  partSize?: number                         // 默认 8MB
  concurrency?: number                      // 默认 4
  onUploadProgress?: (p: number) => void    // 0..1
  onServerStage?: (stage: string, p: number) => void // MERGING/TRANSCODING/DONE/EXISTS...
}

type PreflightResp =
  | { exists: true; objectId: string; downloadUrl?: string; rawUrl?: string }
  | { exists: false; objectId: string; uploadId?: string }

function joinUrl(base: string, path: string) {
  return `${base.replace(/\/$/, "")}${path.startsWith("/") ? "" : "/"}${path}`
}

/**
 * 预检：在下载 TS 之前先问后端是否已经存在同一视频（基于 objectId）。
 * 返回：
 *  - exists=true  => 可直接用 downloadUrl（或 rawUrl）跳过下载+上传
 *  - exists=false => 若后端已分配 uploadId 可直接复用；否则后续再 init
 *
 * 说明：
 *  - 你的 app.py 的 /api/ingestions 已支持“若已存在则直接返回 exists=true 且不提供 uploadId”
 *  - 我们这里在预检时允许 total=0；仅用于“查重”，不影响后续再次 init
 */
export async function preflightIngestion(
  metaLite: Omit<IngestionMeta, "total">,
  opts?: Pick<Opts, "endpoint" | "onServerStage">,
): Promise<PreflightResp> {
  const endpoint = (opts?.endpoint ?? "http://127.0.0.1:5000").replace(/\/$/, "")
  const body: Partial<IngestionMeta> & { __mode?: "preflight" } = {
    ...metaLite,
    total: 0,
    __mode: "preflight", // 后端若忽略该字段也无碍
  }

  const r = await fetch(joinUrl(endpoint, "/api/ingestions"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })

  if (!r.ok) throw new Error(`preflight failed: ${r.status}`)
  const data = await r.json()

  // 兼容：如果后端没有实现真正的 preflight，但仍会返回 exists/objectId
  if (data?.exists === true) {
    const downloadUrl: string | undefined = data.downloadUrl
      ? (/^https?:\/\//.test(data.downloadUrl) ? data.downloadUrl : joinUrl(endpoint, data.downloadUrl))
      : undefined
    const rawUrl: string | undefined = data.rawUrl
      ? (/^https?:\/\//.test(data.rawUrl) ? data.rawUrl : joinUrl(endpoint, data.rawUrl))
      : undefined
    opts?.onServerStage?.("EXISTS", 1)
    return {
      exists: true,
      objectId: data.objectId,
      downloadUrl,
      rawUrl,
    }
  }

  // 不存在：可能给出 uploadId（可复用），也可能没有（则稍后再 init）
  opts?.onServerStage?.("NOT_EXISTS", 0)
  return {
    exists: false,
    objectId: data.objectId,
    uploadId: data.uploadId, // 可能为 undefined
  }
}

/**
 * 旧入口（保持签名不变）：
 * - 仍然从 tsBlob 计算 total → init → 上传 → 完成 → 轮询状态
 * - 可选地传入 preflightResult（若你在外层先做了预检）
 */
export function startIngestion(
  tsBlob: Blob,
  metaLite: Omit<IngestionMeta, "total">,
  opts?: Opts & { preflightResult?: PreflightResp },
): [Promise<{ uploadId?: string; downloadUrl?: string }>, () => void] {
  const endpoint = (opts?.endpoint ?? "http://127.0.0.1:5000").replace(/\/$/, "")
  const partSize = opts?.partSize ?? 8 * 1024 * 1024
  const concurrency = opts?.concurrency ?? 4

  const controllers = new Set<AbortController>()
  let aborted = false
  const cancel = () => {
    aborted = true
    controllers.forEach(c => c.abort())
  }

  const promise = (async () => {
    // --- A) 若外层已预检且存在：直接返回后端链接，跳过上传 ---
    if (opts?.preflightResult?.exists) {
      const url = opts.preflightResult.downloadUrl
      if (url) return { uploadId: undefined, downloadUrl: url }
      // 如果只给了 rawUrl，也一并返回
      const raw = (opts.preflightResult as any).rawUrl as string | undefined
      return { uploadId: undefined, downloadUrl: raw }
    }

    // --- B) 未预检或不存在：开始上传流程 ---
    const total = Math.ceil(tsBlob.size / partSize)
    const meta: IngestionMeta = { ...metaLite, total }

    // 如果预检返回了 uploadId，就直接复用；否则正常 init
    let uploadId = opts?.preflightResult && "uploadId" in opts.preflightResult
      ? (opts.preflightResult.uploadId || "")
      : ""

    if (!uploadId) {
      const initRes = await fetch(joinUrl(endpoint, "/api/ingestions"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(meta),
      })
      if (!initRes.ok) throw new Error(`init failed: ${initRes.status}`)
      const initJson = await initRes.json() as { uploadId?: string; exists?: boolean; downloadUrl?: string }
      if (initJson.exists) {
        // 理论上不会走到这里（因为外层没做预检），但兜底处理
        let url = initJson.downloadUrl
        if (url && !/^https?:\/\//.test(url)) url = joinUrl(endpoint, url)
        opts?.onServerStage?.("EXISTS", 1)
        return { uploadId: undefined, downloadUrl: url }
      }
      if (!initJson.uploadId) throw new Error("server did not return uploadId")
      uploadId = initJson.uploadId
    }

    // --- C) 并发上传分片（1-based） ---
    const indices = Array.from({ length: total }, (_, i) => i + 1)
    let uploaded = 0

    async function uploadOne(i: number) {
      if (aborted) throw new DOMException("Aborted", "AbortError")
      const start = (i - 1) * partSize
      const end = Math.min(i * partSize, tsBlob.size)
      const chunk = tsBlob.slice(start, end)

      let tries = 3
      while (tries--) {
        const c = new AbortController()
        controllers.add(c)
        try {
          const res = await fetch(joinUrl(endpoint, `/api/ingestions/${uploadId}/segments?i=${i}`), {
            method: "POST",
            headers: { "Content-Type": "application/octet-stream" },
            body: chunk,
            signal: c.signal,
          })
          controllers.delete(c)
          if (!res.ok) throw new Error(`segment ${i} http ${res.status}`)
          uploaded++
          opts?.onUploadProgress?.(uploaded / total)
          return
        } catch (e) {
          controllers.delete(c)
          if (aborted) throw e
          if (tries <= 0) throw e
          await new Promise(r => setTimeout(r, 400)) // 简单退避
        }
      }
    }

    // 并发执行
    const queue = indices.slice()
    async function worker() {
      while (queue.length) {
        const i = queue.shift()!
        await uploadOne(i)
      }
    }
    await Promise.all(Array.from({ length: concurrency }, worker))

    // --- D) 标记完成 ---
    const complete = await fetch(joinUrl(endpoint, `/api/ingestions/${uploadId}/complete`), { method: "POST" })
    if (!complete.ok) throw new Error(`complete failed: ${complete.status}`)

    // --- E) 轮询状态（透传 stage） ---
    while (true) {
      if (aborted) throw new DOMException("Aborted", "AbortError")
      const st = await fetch(joinUrl(endpoint, `/api/ingestions/${uploadId}/status`))
      const state = await st.json()
      const stage = state.stage as string
      const p = Number(state.progress ?? 0)
      opts?.onServerStage?.(stage, Number.isFinite(p) ? p : 0)

      if (stage === "DONE") {
        let url: string | undefined = state.downloadUrl
        if (url && !/^https?:\/\//.test(url)) url = joinUrl(endpoint, url)
        return { uploadId, downloadUrl: url }
      }
      if (stage === "FAILED" || stage === "UNKNOWN") {
        throw new Error(`server stage: ${stage} ${state.message || ""}`)
      }
      await new Promise(r => setTimeout(r, 1000))
    }
  })()

  return [promise, cancel]
}
