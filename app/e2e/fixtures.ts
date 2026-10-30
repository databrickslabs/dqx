import { test as base, expect, type APIRequestContext, type Page, type Request } from "@playwright/test";

import { StudioApi } from "./support/studio-api";
import { resolveTarget } from "./target";

/** Collects browser-side failures so specs can assert a page rendered cleanly. */
export interface PageProblems {
  /** Waits until no /api/ request has been in flight for a short quiet period. */
  settle(): Promise<void>;
  /** Returns and clears the problems recorded since the last call. */
  drain(): string[];
}

/** Records what a test created so it is deleted afterwards, even when the test fails. */
export interface Cleanup {
  rule(ruleId: string): void;
  table(bindingId: string): void;
  /** Any other teardown step, run before tables and rules are deleted. */
  defer(label: string, fn: () => Promise<unknown>): void;
}

interface TestFixtures {
  problems: PageProblems;
  cleanup: Cleanup;
}

interface WorkerFixtures {
  /** Authenticated client for the backend REST API; worker-scoped so beforeAll hooks can use it. */
  api: APIRequestContext;
  /** Typed Studio API helpers over *api*. */
  studio: StudioApi;
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
  studio: [async ({ api }, use) => use(new StudioApi(api)), { scope: "worker" }],
  problems: async ({ page }, use) => {
    await use(watchPage(page));
  },
  cleanup: async ({ studio }, use) => {
    const deferred: { label: string; fn: () => Promise<unknown> }[] = [];
    const tables: string[] = [];
    const rules: string[] = [];
    await use({
      rule: (id) => rules.push(id),
      table: (id) => tables.push(id),
      defer: (label, fn) => deferred.push({ label, fn }),
    });
    // Reverse order; tables before rules because a rule applied to a table can't be deleted.
    const steps = [
      ...deferred.reverse(),
      ...tables.reverse().map((id) => ({ label: `delete table ${id}`, fn: () => studio.deleteMonitoredTable(id) })),
      ...rules.reverse().map((id) => ({ label: `delete rule ${id}`, fn: () => studio.deleteRule(id) })),
    ];
    const failures: string[] = [];
    for (const step of steps) {
      await step.fn().catch((err: Error) => failures.push(`${step.label}: ${err.message}`));
    }
    if (failures.length) throw new Error(`cleanup failed (global setup sweeps leftovers next run):\n${failures.join("\n")}`);
  },
});

export { expect };
