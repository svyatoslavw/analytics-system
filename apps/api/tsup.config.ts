import { defineConfig } from "tsup"

export default defineConfig({
  entry: ["src/main.ts"],
  outDir: "dist",
  format: ["esm", "cjs"],
  clean: true,
  sourcemap: true,
  target: "node22",
  dts: false,
  tsconfig: "./tsconfig.json",
  esbuildOptions(options) {
    options.alias = {
      "@": "./src"
    }
  }
})
