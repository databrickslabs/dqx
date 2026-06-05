import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import { defineConfig } from "vite";
import { readFileSync } from "fs";
import { join, resolve } from "path";
import { parse } from "smol-toml";

type AppMetadata = {
  appName: string;
  appSlug: string;
  appModule: string;
};

// Read project metadata from pyproject.toml. ``[tool.dqx_app.metadata]``
// is the single source of truth for app name / slug / FastAPI module;
// ``scripts/build_app.py`` reads the same key to write ``_metadata.py``
// into the wheel. Keeps build-time and bundle-time wiring in lockstep.
function readMetadata(): AppMetadata {
  const pyprojectPath = join(process.cwd(), "pyproject.toml");
  const pyproject = parse(readFileSync(pyprojectPath, "utf-8")) as any;
  const metadata = pyproject?.tool?.dqx_app?.metadata;
  if (!metadata || typeof metadata !== "object") {
    throw new Error(
      "Could not find [tool.dqx_app.metadata] in pyproject.toml",
    );
  }
  return {
    appName: metadata["app-name"],
    appSlug: metadata["app-slug"],
    appModule: metadata["app-module"],
  };
}

export default defineConfig(({ command }) => {
  const { appName: APP_NAME, appSlug: APP_SLUG } = readMetadata();

  const APP_UI_PATH = `./src/${APP_SLUG}/ui`;
  const OUT_DIR = `../__dist__`; // relative to APP_UI_PATH!

  const isDevServer = command === "serve";

  // Backend port for the dev-server proxy. ``scripts/dev.py`` sets this
  // to match the uvicorn ``--port`` it spawns; defaults to 9002 so a
  // standalone ``vite`` invocation still proxies to a conventional
  // FastAPI port without needing the orchestrator script.
  const backendPort = process.env.DQX_APP_BACKEND_PORT ?? "9002";
  const backendTarget = `http://localhost:${backendPort}`;

  return {
    root: APP_UI_PATH,
    publicDir: "./public", // relative to APP_UI_PATH!
    plugins: [
      tanstackRouter({
        target: "react",
        autoCodeSplitting: true,
        routesDirectory: "./routes",
        generatedRouteTree: "./types/routeTree.gen.ts",
      }),
      react(),
      tailwindcss(),
    ],
    server: isDevServer
      ? {
          host: "localhost",
          // Default public-facing port for the dev workflow; overridable
          // via ``vite --port`` (used by scripts/dev.py to keep the
          // documented 9001 URL stable).
          port: 9001,
          strictPort: true,
          // Vite forwards these path prefixes to the FastAPI backend.
          // Anything else is served by Vite itself (the SPA shell, JS
          // assets, HMR machinery). Matches the routing surface the
          // legacy apx dev proxy exposed: /api/* for app routes plus
          // FastAPI's auto-docs at /docs, /redoc, and /openapi.json.
          proxy: {
            "/api": backendTarget,
            "/docs": backendTarget,
            "/redoc": backendTarget,
            "/openapi.json": backendTarget,
          },
        }
      : undefined,
    resolve: {
      alias: {
        "@": resolve(__dirname, APP_UI_PATH),
      },
    },
    build: {
      outDir: OUT_DIR,
      emptyOutDir: true,
    },
    define: {
      __APP_NAME__: JSON.stringify(APP_NAME),
    },
  };
});
