import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import { defineConfig } from "vite";
import { existsSync, readFileSync } from "fs";
import { join, resolve } from "path";
import { parse } from "smol-toml";

type ApxMetadata = {
  appName: string;
  appSlug: string;
  appModule: string;
};

type DevConfig = {
  token_id: string | null;
  pid: number | null;
  port: number | null;
};

type ProjectConfig = {
  dev: DevConfig;
};

type PortsResponse = {
  frontend_port: number;
  backend_port: number;
  host: string;
};

// read metadata from pyproject.toml using toml npm package
export function readMetadata(): ApxMetadata {
  const pyprojectPath = join(process.cwd(), "pyproject.toml");
  const pyproject = parse(readFileSync(pyprojectPath, "utf-8")) as any;

  const metadata = pyproject?.tool?.apx?.metadata;

  if (!metadata || typeof metadata !== "object") {
    throw new Error("Could not find [tool.apx.metadata] in pyproject.toml");
  }

  return {
    appName: metadata["app-name"],
    appSlug: metadata["app-slug"],
    appModule: metadata["app-module"],
  };
}

// read dev server port from .apx/project.json
export function readDevServerPort(): number | null {
  const projectJsonPath = join(process.cwd(), ".apx", "project.json");

  if (!existsSync(projectJsonPath)) {
    return null;
  }

  try {
    const projectConfig: ProjectConfig = JSON.parse(
      readFileSync(projectJsonPath, "utf-8"),
    );
    return projectConfig.dev.port;
  } catch (error) {
    return null;
  }
}

// fetch ports from dev server
export async function fetchPorts(): Promise<{
  frontendPort: number;
  backendPort: number;
  host: string;
}> {
  const devServerPort = readDevServerPort();

  // If no dev server is running, use defaults
  if (!devServerPort) {
    return { frontendPort: 5173, backendPort: 8000, host: "localhost" };
  }

  try {
    const response = await fetch(`http://localhost:${devServerPort}/ports`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data: PortsResponse = await response.json();
    return {
      frontendPort: data.frontend_port,
      backendPort: data.backend_port,
      host: data.host,
    };
  } catch (error) {
    console.warn(
      `Failed to fetch ports from dev server on port ${devServerPort}:`,
      error,
    );
    // Fallback to defaults
    return { frontendPort: 5173, backendPort: 8000, host: "localhost" };
  }
}

// Use async config to fetch ports from dev server
export default defineConfig(async () => {
  const { appName: APP_NAME, appSlug: APP_SLUG } =
    readMetadata() as ApxMetadata;
  const {
    frontendPort: FRONTEND_PORT,
    backendPort: BACKEND_PORT,
    host: HOST,
  } = await fetchPorts();

  const APP_UI_PATH = `./src/${APP_SLUG}/ui`;
  const OUT_DIR = `../__dist__`; // relative to APP_UI_PATH!

  return {
    root: APP_UI_PATH,
    publicDir: "./public", // relative to APP_UI_PATH!
    plugins: [
      tanstackRouter({
        target: "react",
        autoCodeSplitting: true,
        routesDirectory: `${APP_UI_PATH}/routes`,
        generatedRouteTree: "./types/routeTree.gen.ts",
      }),
      react(),
      tailwindcss(),
    ],
    // setup proxy for the api, only used in development
    server: {
      host: HOST,
      port: FRONTEND_PORT,
      strictPort: true,
      proxy: {
        "/api": {
          target: `http://${HOST}:${BACKEND_PORT}`,
          changeOrigin: true,
          secure: false,
        },
      },
    },
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
