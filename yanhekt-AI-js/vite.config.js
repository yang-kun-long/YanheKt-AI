// vite.config.js / vite.config.ts
import { defineConfig } from "vite"
import { resolve } from "node:path"
import solid from "vite-plugin-solid"
import unocss from "unocss/vite"
import { name } from "./package.json"
import userscript from "./vite-plugin-userscript"

export default defineConfig(({ mode }) => {
  const DEV = mode === "development"
  return {
    plugins: [unocss(), solid(), userscript()],
    build: {
      lib: {
        entry: resolve(__dirname, "src/index.tsx"),
        name: "user_script",
        // ✅ 关键：让 userscript 插件包裹 IIFE，lib 本身用 ES
        formats: ["es"],
        fileName: () => "yanhekt-ai-client-proxy.user.js",
      },
      minify: DEV ? false : "esbuild",
      rollupOptions: {
        output: {
          // ✅ 单文件，避免代码分片再拼接出意外
          inlineDynamicImports: true,
          // ✅ 保险：在 bundle 前面插一个分号，防止和头部/上一个块黏连
          intro: ";",
        },
      },
    },
    esbuild: {
      minifyIdentifiers: false,
      minifyWhitespace: false,
      drop: DEV ? [] : ["console", "debugger"],
    },
    resolve: {
      alias: {
        "@assets": resolve(__dirname, "src/assets"),
        "@styles": resolve(__dirname, "src/css"),
      },
    },
  }
})
