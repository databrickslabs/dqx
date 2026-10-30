import type { Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { BAKEHOUSE } from "../../support/sample-tables";
import type { StudioApi } from "../../support/studio-api";
import { knownBug } from "../../support/known-bugs";
import {
  dialogWithHeading,
  expectNewSuccessfulRun,
  expectToast,
  openStudio,
  pickFromCombobox,
  pickManyFromCombobox,
} from "../../support/ui";

// Journeys across many tables at once, owning the whole samples.bakehouse
// schema. A data platform owner monitors the schema with "Select all", rolls
// one registry rule out to the four tables that share a franchiseID column,
// approves and runs them in bulk from the Tables list, compares them on the
// Results page, and stops monitoring the schema in one go. Tests in this file
// run in order.

const [CATALOG, SCHEMA] = BAKEHOUSE.schema.split(".");
const shortName = (fqn: string) => fqn.split(".").pop()!;
const ALL_TABLES = [...BAKEHOUSE.withFranchiseId, ...BAKEHOUSE.others];

/** Ticks the Tables-list row checkbox for each table. */
async function selectRows(page: Page, tables: readonly string[]) {
  for (const fqn of tables) await page.getByRole("checkbox", { name: `Select ${fqn}`, exact: true }).check();
  await expect(page.getByText(`${tables.length} selected`)).toBeVisible();
}

/** Opens the Tables list filtered to the bakehouse schema. */
async function openBakehouseTables(page: Page) {
  await openStudio(page, "/monitored-tables");
  await page.getByPlaceholder(/^Search/).first().fill(`${SCHEMA}.`);
  await expect(page.getByRole("checkbox", { name: `Select ${BAKEHOUSE.withFranchiseId[0]}`, exact: true })).toBeVisible();
}

/** Clicks a bulk-action button and confirms its dialog. */
async function bulkAction(page: Page, action: string, confirmTitle: RegExp) {
  await page.getByRole("button", { name: action, exact: true }).first().click();
  const confirm = page.getByRole("alertdialog");
  await expect(confirm).toContainText(confirmTitle);
  await confirm.getByRole("button", { name: action, exact: true }).click();
}

/** Opens a registry rule's "Apply to tables" dialog and picks the four franchise tables. */
async function applyToFranchiseTables(page: Page, ruleId: string) {
  await openStudio(page, `/registry-rules/${ruleId}`);
  await page.getByRole("button", { name: "Actions" }).click();
  await page.getByRole("menuitem", { name: "Apply to tables" }).click();
  const dialog = dialogWithHeading(page, "Apply to tables");
  await pickFromCombobox(page, "Select catalogs…", CATALOG);
  await pickFromCombobox(page, "Select schemas…", SCHEMA);
  await pickManyFromCombobox(page, "Select tables…", BAKEHOUSE.withFranchiseId.map(shortName));
  // Only selections of more than 10 tables ask "Apply to N tables?" first.
  await dialog.getByRole("button", { name: "Apply", exact: true }).click();
  return dialog;
}

function franchiseRule(studio: StudioApi, label: string, workerIndex: number) {
  // The column variable is named after the column so "Apply to tables" can auto-map it.
  return studio.createPublishedRule({
    name: e2eName(label, workerIndex),
    fn: "is_not_null",
    slot: "franchiseID",
    dimension: "Completeness",
  });
}

test.describe("Multi-table", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test.beforeEach(async ({ studio, cleanup }) => {
    cleanup.defer(`stop monitoring ${BAKEHOUSE.schema}`, async () => {
      for (const t of await studio.listMonitoredTablesIn(BAKEHOUSE.schema)) await studio.deleteMonitoredTable(t.binding_id);
    });
  });

  test("monitor a schema, roll a rule out to it, approve and run the tables in bulk", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(20 * 60_000);
    const rule = await franchiseRule(studio, "franchise-id-present", testInfo.workerIndex);
    cleanup.rule(rule.rule_id);

    await test.step("monitor the whole schema with Select all", async () => {
      await openStudio(page, "/monitored-tables");
      await page.getByRole("button", { name: "Monitor a table" }).first().click();
      const dialog = dialogWithHeading(page, "Monitor tables");
      await pickFromCombobox(page, "Select catalogs…", CATALOG);
      await pickFromCombobox(page, "Select schemas…", SCHEMA);
      await page.getByRole("combobox").filter({ hasText: "Select tables…" }).click();
      await page.getByRole("option", { name: "Select all" }).click();
      await page.keyboard.press("Escape");
      await dialog.getByRole("button", { name: `Add ${ALL_TABLES.length} tables` }).click();
      await expectToast(page, new RegExp(`^${ALL_TABLES.length} registered`));
      await expect
        .poll(async () => (await studio.listMonitoredTablesIn(BAKEHOUSE.schema)).map((t) => t.table_fqn).sort())
        .toEqual([...ALL_TABLES].sort());
    });

    const franchiseTables = (await studio.listMonitoredTablesIn(BAKEHOUSE.schema)).filter((t) =>
      (BAKEHOUSE.withFranchiseId as readonly string[]).includes(t.table_fqn),
    );

    await test.step("each owner applies the rule to their franchise table", async () => {
      // "Apply to tables" only offers tables that aren't monitored yet (and is
      // broken for those — see the next test), so per-table application is set
      // up through the API here; the per-table UI flow is covered in
      // monitored-tables.spec.ts.
      for (const t of franchiseTables) await studio.applyRule(t.binding_id, rule.rule_id, "franchiseID", "franchiseID");
    });

    await test.step("approve the four tables in bulk", async () => {
      // Each owner submits their table; the approver then clears the queue at once.
      for (const t of franchiseTables) await studio.submitTable(t.binding_id);
      await openBakehouseTables(page);
      await selectRows(page, BAKEHOUSE.withFranchiseId);
      await bulkAction(page, "Approve", /^Approve 4 tables\?/);
      for (const t of franchiseTables) {
        await expect.poll(async () => (await studio.getMonitoredTable(t.binding_id)).status, t.table_fqn).toBe("approved");
      }
    });

    const runsBefore = new Map<string, Set<string>>();
    for (const fqn of BAKEHOUSE.withFranchiseId) runsBefore.set(fqn, await studio.runIdsFor(fqn));

    await test.step("run the four tables in bulk", async () => {
      await openBakehouseTables(page);
      await selectRows(page, BAKEHOUSE.withFranchiseId);
      await bulkAction(page, "Run", /^Run 4 tables\?/);
      // The runs are submitted one table at a time.
      await expectToast(page, /^Run started/, 90_000);
    });

    await test.step("all four runs succeed in Runs History", async () => {
      for (const fqn of BAKEHOUSE.withFranchiseId) await expectNewSuccessfulRun(page, studio, fqn, runsBefore.get(fqn)!);
    });

    await test.step("the Results page compares the tables", async () => {
      await openStudio(page, "/results");
      for (const fqn of BAKEHOUSE.withFranchiseId) {
        await expect(page.getByText(fqn).or(page.getByText(shortName(fqn))).first(), fqn).toBeVisible({ timeout: 60_000 });
      }
    });

    await test.step("stop monitoring the whole schema in bulk", async () => {
      await openBakehouseTables(page);
      await selectRows(page, ALL_TABLES);
      await bulkAction(page, "Delete", new RegExp(`^Delete ${ALL_TABLES.length} tables\\?`));
      await expect.poll(async () => (await studio.listMonitoredTablesIn(BAKEHOUSE.schema)).length).toBe(0);
    });
  });

  test("apply a rule to tables that aren't monitored yet", async ({ page, studio, cleanup }, testInfo) => {
    const rule = await franchiseRule(studio, "franchise-id-new-tables", testInfo.workerIndex);
    cleanup.rule(rule.rule_id);

    await applyToFranchiseTables(page, rule.rule_id);
    // Known Studio bug: the picker only offers unmonitored tables. "Apply to
    // tables" registers them, then looks up their binding ids in the
    // monitored-tables list it already had cached from the picker
    // (ApplyRuleModal runApply -> queryClient.fetchQuery returns the
    // 5-minute-fresh cache), so none are found and every table is reported as
    // "needs column mapping": the rule is never applied.
    await knownBug('"Apply to tables" never applies the rule to newly registered tables', async () => {
      await expectToast(page, "Applied to 4 table(s); 0 need column mapping");
    });
  });
});
