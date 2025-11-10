// src/App.tsx
import { batch, createSignal, type Component } from "solid-js"
import { createStore, produce, unwrap } from "solid-js/store"
import { DownloadButtons } from "./components/Button"
import { TaskCreatePanel, PanelState } from "./components/Panel"
import { TaskSnackbarHost, DownloadTask } from "./components/Snackbar"

const App: Component = () => {
  const [panelState, setPanelState] = createSignal<PanelState>()
  const [panelVisible, setPanelVisible] = createSignal(false)
  const [tasks, setTasks] = createStore<DownloadTask[]>([])

  // --- 1. 新增：创建 stages 状态 ---
  //    用于存储每个课程（key: courseTitle）的当前服务器状态（value: stage）
  const [stages, setStages] = createSignal(new Map<string, string>())

  const addTask = (task: DownloadTask) => {
    setTasks(tasks.length, task)
  }
  const deleteTask = (index: number) => {
    setTasks(
      produce((tasks) => {
        tasks.splice(index, 1)
      }),
    )
  }
  const restartTask = (index: number) => {
    const task = unwrap(tasks)[index]!
    deleteTask(index)
    addTask(task)
  }
  const showPanel = (panelState: PanelState) => {
    batch(() => {
      setPanelState(panelState)
      setPanelVisible(true)
    })
  }

  // --- 2. 新增：创建 handleStageChange ---
  //    这个函数将作为 prop 传给 TaskSnackbarHost，
  //    以便子组件（Snackbar）能把轮询到的状态（stage）报告上来。
  //    我们假设 DownloadTask 类型中包含 'title' 字段。
  const handleStageChange = (index: number, stage: string) => {
    const task = tasks[index] // 从 store 中读取
    
    // 假设 task 对象上有 title 属性，该属性对应 courseTitle
    const title = task?.courseTitle
    
    if (title) {
      setStages(prevMap => {
        // 优化：如果按钮已经是 "DONE"，则不覆盖
        if (prevMap.get(title) === "DONE") {
          return prevMap
        }
        const newMap = new Map(prevMap)
        newMap.set(title, stage)
        console.log("[App] Stage updated:", { title, stage })
        return newMap
      })
    }
  }

  return (
    <>
      <DownloadButtons
        // --- 3. 修改：将 stages 状态传递下去 ---
        stages={stages()}
        onClick={(courseTitle) => {
          const fullName =
            document
              .querySelector(".course-intro-title")
              ?.textContent?.trim() ?? "未知课程"
          const courseName = fullName.substring(0, fullName.indexOf("("))
          console.log("[App] Button clicked:", { courseTitle })
          // 检查是否已经是 DONE
          if (stages().get(courseTitle) === "DONE") {
            console.log("该课程已洞悉完成，跳过创建。")
            return
          }

          showPanel({ courseName, title: courseTitle })
        }}
      />
      <TaskCreatePanel
        visible={panelVisible()}
        uiState={panelState()}
        onClose={() => {
          setPanelVisible(false)
        }}
        onCreate={(task) => {
          console.log("Create", task)
          setPanelVisible(false)
          addTask(task)
        }}
      />
      <TaskSnackbarHost
        tasks={tasks}
        onComplete={deleteTask}
        onRetry={restartTask}
        // --- 4. 修改：传递 onStageChange 回调 ---
        onStageChange={handleStageChange}
      />
    </>
  )
}

export default App