import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const proxyTarget = env.VITE_BFF_PROXY_TARGET;

  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": "/src",
      },
    },
    server: {
      proxy: proxyTarget
        ? {
            "/api": {
              target: proxyTarget,
              changeOrigin: true,
            },
          }
        : undefined,
    },
  };
});
