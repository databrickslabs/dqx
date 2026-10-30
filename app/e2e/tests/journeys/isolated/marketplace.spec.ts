import { expect, test } from "../../../fixtures";
import { escapeRegExp, expectToast, openStudio } from "../../../support/ui";

// Journey: an admin browses the rule marketplace and adds a ready-made rule to
// the registry. Marketplace rules keep their own names (no e2e prefix), so the
// test picks one that isn't imported yet and deletes it afterwards.

interface Pack {
  title: string;
  rules: { name: string; imported: boolean }[];
}

test.describe("Marketplace", () => {
  test("add a marketplace rule to the registry", async ({ page, api, studio, cleanup }) => {
    const res = await api.get("/api/v1/marketplace/packs");
    expect(res.status()).toBe(200);
    const { packs } = (await res.json()) as { packs: Pack[] };
    const candidate = packs.flatMap((p) => p.rules.map((r) => ({ pack: p.title, ...r }))).find((r) => !r.imported);
    test.skip(!candidate, "every marketplace rule is already imported on this deployment");
    const { name } = candidate!;

    await openStudio(page, "/marketplace");
    await expect(page.getByRole("heading", { name: "Rule Marketplace" })).toBeVisible();
    await page.getByPlaceholder("Search rules…").fill(name);
    await page.getByRole("checkbox", { name: `Select ${name}`, exact: true }).check();
    await page.getByRole("button", { name: "Add 1 to registry" }).click();
    await expectToast(page, /^Imported 1 rule/);

    const rule = await studio.findRuleByName(name);
    expect(rule, `imported rule "${name}"`).toBeDefined();
    cleanup.rule(rule!.rule_id);
    // Marketplace imports are auto-approved, so they are ready to apply.
    expect(rule!.status).toBe("approved");

    await page.reload();
    await page.getByPlaceholder("Search rules…").fill(name);
    await expect(page.getByRole("checkbox", { name: new RegExp(`^Select ${escapeRegExp(name)}$`) })).toBeDisabled();
  });
});
