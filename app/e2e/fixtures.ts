import { test as base, expect, type APIRequestContext, type Page, type Request } from "@playwright/test";

import { resolveTarget } from "./target";

/** Collects browser-side failures so specs can assert a page rendered cleanly. */
export interface PageProblems {
  /** Waits until no /api/ request has been in flight for a short quiet period. */
  settle(): Promise<void>;
  /** Returns and clears the problems recorded since the last call. */
  drain(): string[];
}

interface TestFixtures {
  problems: PageProblems;
}

interface WorkerFixtures {
  /** Authenticated client for the backend REST API; worker-scoped so beforeAll hooks can use it. */
  api: APIRequestContext;
}

function authHeaders(): Record<string, string> {
  const { token } = resolveTarget();
  return token ? { authorization: `Bearer ${token}` } : {};
}

function watchPage(page: Page): PageProblems {
  const problems: string[] = [];
  const inflight = new Set<Request>();
  let lastActivity = Date.now();
  const isApi = (url: string) => new URL(url).pathname.startsWith("/api/");
  const label = (req: Request) => `${req.method()} ${new URL(req.url()).pathname}`;

  page.on("pageerror", (err) => problems.push(`uncaught error: ${err.message}`));
  page.on("request", (req) => {
    if (isApi(req.url())) {
      inflight.add(req);
      lastActivity = Date.now();
    }
  });
  const done = (req: Request) => {
    if (inflight.delete(req)) lastActivity = Date.now();
  };
  page.on("requestfinished", done);
  page.on("requestfailed", done);
  // A full navigation cancels the previous document's requests. Ones cancelled while
  // paused in the auth route handler never emit requestfinished/requestfailed, so
  // forget them once the new document loads (SPA route changes don't fire this).
  page.on("domcontentloaded", () => inflight.clear());
  // 4xx is expected for role-gated endpoints; 5xx means the backend broke.
  page.on("response", (res) => {
    if (isApi(res.url()) && res.status() >= 500) {
      problems.push(`${res.status()} ${res.request().method()} ${new URL(res.url()).pathname}`);
    }
  });

  return {
    async settle() {
      await expect
        .poll(() => (inflight.size > 0 ? [...inflight].map(label) : Date.now() - lastActivity > 500 ? [] : ["(settling)"]), {
          message: "API requests still in flight",
          intervals: [100],
          timeout: 30_000,
        })
        .toEqual([]);
    },
    drain() {
      return problems.splice(0, problems.length);
    },
  };
}

export const test = base.extend<TestFixtures, WorkerFixtures>({
  context: async ({ context, baseURL }, use) => {
    const headers = authHeaders();
    if (baseURL && headers.authorization) {
      // Scope the token to the app origin so it is never sent to third-party hosts
      // (extraHTTPHeaders would attach it to every request the page makes).
      const origin = new URL(baseURL).origin;
      await context.route(`${origin}/**`, (route) =>
        route.continue({ headers: { ...route.request().headers(), ...headers } }),
      );
    }
    await use(context);
  },
  api: [
    async ({ playwright }, use) => {
      const api = await playwright.request.newContext({
        baseURL: resolveTarget().baseURL,
        extraHTTPHeaders: authHeaders(),
      });
      await use(api);
      await api.dispose();
    },
    { scope: "worker" },
  ],
  problems: async ({ page }, use) => {
    await use(watchPage(page));
  },
});

export { expect };
