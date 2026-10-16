import { describe, expect, test } from "bun:test";
import { IMPORT_EXAMPLES, getImportExampleUrl } from "./ImportExampleLinks";

describe("import examples", () => {
  test("rules and contract examples resolve to different bundled files", () => {
    expect(IMPORT_EXAMPLES.rulesYaml).not.toBe(IMPORT_EXAMPLES.dataContract);
    expect(getImportExampleUrl(IMPORT_EXAMPLES.rulesYaml)).toBe("/examples/imports/rules/bakehouse-rules.yaml");
    expect(getImportExampleUrl(IMPORT_EXAMPLES.dataContract)).toBe(
      "/examples/imports/contracts/bakehouse-sales-contract.yaml",
    );
  });
});
