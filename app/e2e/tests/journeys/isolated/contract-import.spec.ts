import { expect, test } from "../../../fixtures";
import { runTag } from "../../../support/names";
import { SAMPLE_TABLES } from "../../../support/sample-tables";
import { openStudio } from "../../../support/ui";

// Journey: a data product owner imports an ODCS data contract. Studio registers
// the table, creates rules from the contract's constraints and applies them.
// Contract-generated rules are named by Studio (no e2e prefix), so cleanup
// deletes exactly the rules that did not exist before the import.

const TABLE = SAMPLE_TABLES.contractImport;
const [CATALOG, SCHEMA, TABLE_NAME] = TABLE.split(".");

function contract(): string {
  return `kind: DataContract
apiVersion: v3.0.2
id: urn:datacontract:e2e:${runTag()}:orders
name: E2E Orders ${runTag()}
version: 1.0.0
status: active
schema:
  - name: ${TABLE_NAME}
    physicalName: ${TABLE}
    physicalType: table
    properties:
      - name: o_orderkey
        logicalType: integer
        physicalType: BIGINT
        required: true
        unique: true
        primaryKey: true
      - name: o_orderstatus
        logicalType: string
        physicalType: STRING
        required: true
        logicalTypeOptions:
          pattern: '^[FOP]$'
      - name: o_totalprice
        logicalType: number
        physicalType: DECIMAL(18,2)
        required: true
`;
}

test.describe("Data contract import", () => {
  test("import a contract to register a table with generated rules", async ({ page, studio, cleanup }) => {
    test.setTimeout(240_000);
    const before = new Set((await studio.listRules()).map((r) => r.rule_id));
    // One step so the table (and its applied rules) goes before the rules themselves.
    cleanup.defer(`stop monitoring ${TABLE}, then delete rules created by the import`, async () => {
      const table = await studio.findMonitoredTable(TABLE);
      if (table) await studio.deleteMonitoredTable(table.binding_id);
      for (const rule of await studio.listRules()) if (!before.has(rule.rule_id)) await studio.deleteRule(rule.rule_id);
    });

    await test.step("upload the contract and choose the target schema", async () => {
      await openStudio(page, "/monitored-tables/import");
      await expect(page.getByRole("heading", { name: "Import to tables" })).toBeVisible();
      await page.locator('input[type="file"]').first().setInputFiles({
        name: "e2e-orders-contract.yaml",
        mimeType: "application/x-yaml",
        buffer: Buffer.from(contract()),
      });
      await page.getByRole("combobox").filter({ hasText: "Select catalog" }).click();
      await page.getByRole("option", { name: CATALOG, exact: true }).click();
      await page.getByRole("combobox").filter({ hasText: "Select schema" }).click();
      await page.getByRole("option", { name: SCHEMA, exact: true }).click();
    });

    await test.step("preview and run the import", async () => {
      await page.getByRole("button", { name: "Preview plan" }).click();
      // The plan replaces the upload card inline (it is not a dialog).
      await expect(page.getByText("Import plan")).toBeVisible();
      await expect(page.getByRole("row").filter({ hasText: TABLE })).toContainText("New");
      // A single-table import runs straight away; multi-table imports ask "Import into N tables?" first.
      await page.getByRole("button", { name: "Register, create & apply" }).click();
      await expect(page.getByText("Import results")).toBeVisible({ timeout: 120_000 });
      const summary = page.getByRole("button").filter({ hasText: "e2e-orders-contract.yaml" });
      await expect(summary).toContainText("1 registered");
      await expect(summary).toContainText(/\d+ created/);
    });

    await test.step("the table is registered and the new rules await approval", async () => {
      const table = await studio.findMonitoredTable(TABLE);
      expect(table, `${TABLE} registered`).toBeDefined();
      const created = (await studio.listRules()).filter((r) => !before.has(r.rule_id));
      // Constraints that match an existing published rule are reused instead of created.
      expect(created.length).toBeGreaterThan(0);
      // Only approved rules are auto-applied, so new contract rules wait for review first.
      for (const rule of created) expect(rule.status).toBe("pending_approval");
    });
  });
});
