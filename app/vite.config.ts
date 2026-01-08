import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import { defineConfig, type Plugin } from "vite";
import { readFileSync } from "fs";
import { join, resolve } from "path";
import { parse } from "smol-toml";
import type { IncomingMessage, ServerResponse } from "http";

// Header that the APX dev server adds to requests to verify they come from the proxy
const APX_DEV_PROXY_HEADER = "x-apx-dev-proxy";

type ApxMetadata = {
  appName: string;
  appSlug: string;
  appModule: string;
};

// Vite plugin to verify requests come from the APX dev server proxy
function apxDevProxyGuard(): Plugin {
  return {
    name: "apx-dev-proxy-guard",
    configureServer(server) {
      // Add middleware at the start to check for the proxy header
      server.middlewares.use(
        (req: IncomingMessage, res: ServerResponse, next) => {
          // Allow internal Vite requests (HMR, etc.)
          const url = req.url || "";
          if (
            url.startsWith("/@") ||
            url.startsWith("/__vite") ||
            url.startsWith("/node_modules")
          ) {
            next();
            return;
          }

          // Check for the APX dev proxy header
          const hasProxyHeader = req.headers[APX_DEV_PROXY_HEADER] === "true";
          if (!hasProxyHeader) {
            // Redirect to APX dev server instead of returning 403
            const devServerPort = process.env.APX_DEV_SERVER_PORT;
            if (devServerPort) {
              const redirectUrl = `http://localhost:${devServerPort}${url}`;
              res.statusCode = 302;
              res.setHeader("Location", redirectUrl);
              res.end();
            } else {
              // Fallback to 403 if dev server port is not set
              res.statusCode = 403;
              res.setHeader("Content-Type", "text/plain");
              res.end(
                "Direct access to Vite dev server is not allowed. " +
                  "Please access through the APX dev server proxy.",
              );
            }
            return;
          }

          next();
        },
      );
    },
  };
}

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

// Get port configuration from environment variables (set by APX dev server)
export function getPortConfig(): { frontendPort: number; host: string } {
  const frontendPortEnv = process.env.APX_FRONTEND_PORT;

  if (!frontendPortEnv) {
    throw new Error(
      "APX_FRONTEND_PORT environment variable is not set. " +
        "Please start the development server using 'apx dev' command.",
    );
  }

  const frontendPort = parseInt(frontendPortEnv, 10);
  if (isNaN(frontendPort)) {
    throw new Error(
      `Invalid APX_FRONTEND_PORT value: ${frontendPortEnv}. Expected a number.`,
    );
  }

  return {
    frontendPort,
    host: "localhost",
  };
}

// Use sync config since we read from env vars
export default defineConfig(({ command }) => {
  const { appName: APP_NAME, appSlug: APP_SLUG } =
    readMetadata() as ApxMetadata;

  const APP_UI_PATH = `./src/${APP_SLUG}/ui`;
  const OUT_DIR = `../__dist__`; // relative to APP_UI_PATH!

  // Port config is only needed for dev server, not for production build
  const isDevServer = command === "serve";
  const serverConfig = isDevServer
    ? (() => {
        const { frontendPort, host } = getPortConfig();
        return {
          host,
          port: frontendPort,
          strictPort: true,
        };
      })()
    : undefined;

  return {
    root: APP_UI_PATH,
    publicDir: "./public", // relative to APP_UI_PATH!
    plugins: [
      tanstackRouter({
        target: "react",
        autoCodeSplitting: true,
        routesDirectory: `./routes`,
        generatedRouteTree: "./types/routeTree.gen.ts",
      }),
      react(),
      tailwindcss(),
      // Only add the proxy guard plugin in dev mode
      ...(isDevServer ? [apxDevProxyGuard()] : []),
    ],
    server: serverConfig,
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
