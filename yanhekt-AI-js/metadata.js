// @ts-check
import fs from "node:fs"
import path from "node:path"

const packageJson = JSON.parse(
  fs
    .readFileSync(path.resolve(import.meta.dirname, "./package.json"))
    .toString(),
)

const metadata = {
  name: "延河课堂洞察引擎C端代理",
  namespace: packageJson.author,
  version: packageJson.version,
  description: "延河课堂洞察引擎的C端代理，作为“沉浸式数据代理”运行在课程列表页。它是系统的数据发起者，负责**“抓取-上传”**管线。",
  license: packageJson.license,
  match: "https://www.yanhekt.cn/course/*",
  icon: "https://www.yanhekt.cn/yhkt.ico",
   supportURL: "https://github.com/yang-kun-long/YanheKt-AI/issues",
  homepageURL: "https://github.com/yang-kun-long/YanheKt-AI#readme",
  "inject-into": "content",
  unwrap: true,
  noframes: true,
}

export default metadata
