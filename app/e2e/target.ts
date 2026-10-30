import { execFileSync } from "node:child_process";

// Resolves which Studio deployment the suite runs against and how to
// authenticate to it. Runs once in the Playwright main process; the result is
// exported through process.env so worker processes reuse it instead of
// re-invoking the Databricks CLI.
//
//   DQX_E2E_PROFILE   Databricks CLI profile (required unless DQX_E2E_BASE_URL is local)
//   DQX_E2E_APP       Databricks App name (default: dqx-studio)
//   DQX_E2E_BASE_URL  Explicit app URL; skips the apps lookup (e.g. http://localhost:9001)

export interface E2ETarget {
  baseURL: string;
  /**
   * OAuth bearer token at resolution time; undefined for local dev servers.
   * Use *bearerToken()* for requests — it refreshes the token before it expires.
   */
  token: string | undefined;
}

const DEFAULT_APP_NAME = "dqx-studio";

function databricksJson(args: string[], profile: string): Record<string, unknown> {
  try {
    const out = execFileSync("databricks", [...args, "-p", profile, "-o", "json"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return JSON.parse(out) as Record<string, unknown>;
  } catch (err) {
    const stderr = (err as { stderr?: string }).stderr?.trim();
    throw new Error(`'databricks ${args.join(" ")} -p ${profile}' failed${stderr ? `: ${stderr}` : ""}`);
  }
}

function isLocal(url: string): boolean {
  const { hostname } = new URL(url);
  return hostname === "localhost" || hostname === "127.0.0.1";
}

function resolveAppUrl(appName: string, profile: string): string {
  const app = databricksJson(["apps", "get", appName], profile);
  const state = (app.app_status as { state?: string } | undefined)?.state;
  if (state !== "RUNNING") {
    throw new Error(
      `App '${appName}' is not running (state: ${state ?? "unknown"}). ` +
        `Start it with 'databricks bundle run dqx-studio -p ${profile} -t <target>'.`,
    );
  }
  if (typeof app.url !== "string" || !app.url) {
    throw new Error(`App '${appName}' has no URL yet; wait for its deployment to finish.`);
  }
  return app.url;
}

/** Refresh when less than this is left, so no request goes out with a token about to expire. */
const TOKEN_REFRESH_MARGIN_MS = 10 * 60_000;

function fetchToken(profile: string, forceRefresh: boolean): { token: string; expiresAt: number } {
  // The Apps proxy accepts workspace OAuth tokens (U2M or M2M), not PATs.
  const body = databricksJson(["auth", "token", ...(forceRefresh ? ["--force-refresh"] : [])], profile);
  const token = body.access_token;
  if (typeof token !== "string" || !token) {
    throw new Error(
      `Profile '${profile}' did not return an OAuth access token. ` +
        `Databricks Apps require OAuth; run 'databricks auth login -p ${profile}'.`,
    );
  }
  const expiry = typeof body.expiry === "string" ? Date.parse(body.expiry) : NaN;
  return { token, expiresAt: Number.isNaN(expiry) ? Date.now() + 30 * 60_000 : expiry };
}

/**
 * Current bearer token for the Apps proxy, or undefined for a local dev server.
 * Re-fetched from the Databricks CLI when it's close to expiry, so suites longer
 * than the token's lifetime keep working. Call it per request; don't cache it.
 */
export function bearerToken(): string | undefined {
  const env = process.env;
  resolveTarget();
  if (!env.DQX_E2E_RESOLVED_TOKEN) return undefined;
  if (Date.now() > Number(env.DQX_E2E_RESOLVED_TOKEN_EXPIRES_AT) - TOKEN_REFRESH_MARGIN_MS) {
    const fresh = fetchToken(env.DQX_E2E_PROFILE!, true);
    env.DQX_E2E_RESOLVED_TOKEN = fresh.token;
    env.DQX_E2E_RESOLVED_TOKEN_EXPIRES_AT = String(fresh.expiresAt);
  }
  return env.DQX_E2E_RESOLVED_TOKEN;
}

export function resolveTarget(): E2ETarget {
  const env = process.env;
  if (env.DQX_E2E_RESOLVED_BASE_URL) {
    return { baseURL: env.DQX_E2E_RESOLVED_BASE_URL, token: env.DQX_E2E_RESOLVED_TOKEN || undefined };
  }

  // `make app-e2e` exports unset variables as empty strings, so treat "" as unset.
  const profile = env.DQX_E2E_PROFILE || undefined;
  const explicitUrl = env.DQX_E2E_BASE_URL || undefined;
  let target: E2ETarget;
  if (explicitUrl && isLocal(explicitUrl)) {
    // Local dev servers have no Apps proxy; the backend falls back to the CLI identity.
    target = { baseURL: explicitUrl, token: undefined };
  } else {
    if (!profile) {
      throw new Error("Set DQX_E2E_PROFILE (make app-e2e PROFILE=<databricks-profile>).");
    }
    const baseURL = explicitUrl ?? resolveAppUrl(env.DQX_E2E_APP || DEFAULT_APP_NAME, profile);
    // Start from a fresh token: a cached one may be most of the way through its hour.
    const { token, expiresAt } = fetchToken(profile, true);
    env.DQX_E2E_RESOLVED_TOKEN_EXPIRES_AT = String(expiresAt);
    target = { baseURL, token };
  }

  env.DQX_E2E_RESOLVED_BASE_URL = target.baseURL;
  env.DQX_E2E_RESOLVED_TOKEN = target.token ?? "";
  return target;
}
