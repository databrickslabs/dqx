import { readFileSync } from "node:fs";
import { join } from "node:path";

import { expect, test } from "../../fixtures";
import { openStudio, sidebarLinks } from "../../support/ui";

// Read-only smoke checks for a deployed Studio. Safe to run against any
// deployment: nothing here creates, edits, or runs anything.
//
// The target must be a real DQX Studio instance with setup finished (catalog,
// prefix, user group, task runner). There is no mocked backend; until setup is
// complete the "ready deployment" tests fail and list the pending steps.

function expectedStudioVersion(): string {
  const pyproject = readFileSync(join(__dirname, "..", "..", "..", "pyproject.toml"), "utf8");
  const match = /^version = "([^"]+)"$/m.exec(pyproject);
  if (!match) throw new Error("Could not read version from app/pyproject.toml");
  return match[1];
}

test("backend is healthy", async ({ api }) => {
  const res = await api.get("/api/health");
  expect(res.status()).toBe(200);
  expect(await res.json()).toEqual({ status: "ok" });
});

test("deployed Studio version matches app/pyproject.toml", async ({ api }) => {
  const res = await api.get("/api/v1/version");
  expect(res.status()).toBe(200);
  const body = (await res.json()) as { version: string; core_version: string };
  expect(body.version, "the deployment is not running the version you are about to tag").toBe(
    expectedStudioVersion(),
  );
  expect(body.core_version).not.toBe("");
});

// Everything below needs a deployment whose setup has finished: until then the
// setup gate answers 503 for almost every API route.
test.describe("ready deployment", () => {
  test.beforeAll(async ({ api }) => {
    const res = await api.get("/api/v1/setup/status");
    expect(res.status()).toBe(200);
    const { report } = (await res.json()) as {
      report: { state: string; steps: { id: string; state: string; summary: string }[] };
    };
    const pending = report.steps
      .filter((s) => s.state !== "passed" && s.state !== "overridden")
      .map((s) => `  ${s.id}: ${s.summary}`);
    expect(report.state, `setup is not complete; finish it in the Studio UI first:\n${pending.join("\n")}`).toBe(
      "ready",
    );
  });

  test("current user identity and role resolve", async ({ api }) => {
    const res = await api.get("/api/v1/current-user/role");
    expect(res.status()).toBe(200);
    const body = (await res.json()) as { email: string; role: string };
    expect(body.email).toContain("@");
    expect(["admin", "rule_approver", "rule_author", "viewer"]).toContain(body.role);
  });

  // Walks ~12 pages against a real backend; give it more than the default budget.
  test.setTimeout(180_000);

  test("every sidebar page renders without errors", async ({ page, problems }) => {
    await openStudio(page);
    const hrefs = await sidebarLinks(page).evaluateAll((links) => links.map((a) => a.getAttribute("href") ?? ""));
    const pages = [...new Set(["/home", ...hrefs.filter((h) => h.startsWith("/")), "/settings", "/profile"])];
    // The visible set depends on role and approvals mode; a handful is always present.
    expect(pages.length).toBeGreaterThan(4);

    for (const path of pages) {
      await test.step(path, async () => {
        await page.goto(path);
        await expect(page.locator("main")).toBeVisible();
        await problems.settle();
        expect.soft(problems.drain(), `problems on ${path}`).toEqual([]);
      });
    }
  });
});
