import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

/** Where `/api` is proxied in development: the real `tape serve`, or the mock on 8787. */
const apiTarget = process.env["TAPE_API"] ?? "http://127.0.0.1:8080";

const apiProxy = {
  "/api": { target: apiTarget, changeOrigin: true, ws: true },
};

// Vite loads its configuration from the module's default export; config files are the
// only modules allowed one (docs/ENGINEERING_STANDARDS.md section 6).
export default defineConfig({
  plugins: [react()],
  server: { host: "127.0.0.1", port: 5173, strictPort: true, proxy: apiProxy },
  preview: { host: "127.0.0.1", port: 4173, strictPort: true, proxy: apiProxy },
  build: { sourcemap: true },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts", "dev/**/*.test.ts"],
  },
});
