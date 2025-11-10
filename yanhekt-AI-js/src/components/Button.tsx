// src/components/Button.tsx
import {
  Component,
  createSignal,
  For,
  onMount,
  ParentComponent,
} from "solid-js"
import { Portal } from "solid-js/web"
import downloadSVG from "@assets/icons/download.svg"
// 1. 假设你有一个 "完成" 状态的图标
import finishedSvg from "@assets/icons/finished.svg" 

export const DownloadButtons: Component<{
  onClick: (courseTitle: string) => void
  stages: Map<string, string> // 2. 接收一个 Map 来跟踪每个课程的状态
}> = (props) => {
  const [courseNodes, setCourseNodes] = createSignal<Node[]>([])

  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  onMount(async () => {
    const courseListEl = await getCourseListElAsync()

    setCourseNodes(Array.from(courseListEl.children))

    listenItemsMutation(courseListEl, (mutations) => {
      setCourseNodes((prev) => {
        const removed: Node[] = []
        for (const { addedNodes, removedNodes } of mutations) {
          // console.log(addedNodes, removedNodes)
          prev.push(...Array.from(addedNodes))
          removed.push(...Array.from(removedNodes))
        }
        return prev.filter((node) => !removed.includes(node))
      })
    })
  })

  return (
    <div>
      <For each={courseNodes()}>
        {(node) => {
          // 3. 提前获取 title，用于 onClick 和 状态查询
          const title = (node as HTMLElement).querySelector(
            "h4 > span:nth-child(1)",
          )?.textContent
          
          // 4. 从 props.stages 中获取当前课程的状态
          //    使用函数使其保持响应性
          const stage = () => title ? props.stages.get(title) : undefined

          return (
            <Portal mount={node}>
              <DownloadButton
                onClick={() => {
                  if (!title) {
                    console.error("Title not found", node)
                    return
                  }
                  props.onClick(title)
                }}
                // 5. 将 stage 传递给子组件
                stage={stage()}
              >
                {/* 6. 根据 stage 动态显示文本 */}
                {stage() === "DONE" ? "洞悉完成" : "开始洞悉"}
              </DownloadButton>
            </Portal>
          )
        }}
      </For>
    </div>
  )
}

const DownloadButton: ParentComponent<{
  onClick?: (ev: MouseEvent) => void
  stage?: string // 7. 接收 stage prop
}> = (props) => {
  
  const isDone = () => props.stage === "DONE"

  return (
    <button
      type="button"
      // 8. 动态更改 class 来改变颜色
      class="ant-btn ant-btn-round inline-flex items-center gap-[6px]"
      classList={{
        "ant-btn-primary": !isDone(), // 非 DONE 状态为蓝色
        "ant-btn-default": isDone(),  // DONE 状态为白色/灰色
      }}
      onClick={(ev) => props.onClick?.(ev)}
      // 9. "DONE" 状态下禁用按钮
      disabled={isDone()}
    >
      {/* 10. 根据 stage 动态显示图标 */}
      <img 
        width="14" 
        src={isDone() ? finishedSvg : downloadSVG} 
        alt={isDone() ? "completed" : "download"} 
      />
      <span class="hidden! md:inline!">{props.children}</span>
    </button>
  )
}

// --- 以下函数保持不变 ---

function getCourseListElAsync() {
  return new Promise<HTMLElement>((resolve) => {
    const _ = setInterval(() => {
      const elem = document.querySelector(
        ".course-detail .ant-list-items",
      ) as HTMLElement
      if (elem) {
        clearInterval(_)
        resolve(elem)
      }
    }, 500)
  })
}

let observer: MutationObserver

function listenItemsMutation(
  courseListEl: HTMLElement,
  onChange: (mutations: MutationRecord[]) => void,
) {
  observer = new MutationObserver((mutations) => {
    console.log("Mutation")
    onChange(mutations)
  })
  observer.observe(courseListEl, { childList: true })
}