import path from "path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    tanstackRouter({
      target: "react",
      autoCodeSplitting: true,
    }),
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "../src/databricks/labs/dqx/app/static/dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes("react")) {
            return "react";
          }
        },
      },
    },
  },
});
