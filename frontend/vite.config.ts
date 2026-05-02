/// <reference types="vitest/config" />

import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const apiTarget = env.VITE_UPPI_API_BASE_URL?.trim() || "http://localhost:8000";

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/auth": {
          target: apiTarget,
          changeOrigin: true,
        },
        "/attestazioni": {
          target: apiTarget,
          changeOrigin: true,
        },
        "/clients": {
          target: apiTarget,
          changeOrigin: true,
        },
        "/health": {
          target: apiTarget,
          changeOrigin: true,
        },
      },
    },
    test: {
      environment: "jsdom",
      globals: true,
      css: true,
      setupFiles: "./src/test/setup.ts",
    },
  };
});
