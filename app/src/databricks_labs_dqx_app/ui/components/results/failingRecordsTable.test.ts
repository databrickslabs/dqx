import { describe, expect, it } from "bun:test";
import { clampTooltip, dedupeTooltipFailures } from "./FailingRecordsTable";
import { toFailingRecords } from "./failedRecordsExport";

describe("clampTooltip", () => {
  const VW = 1024;
  const VH = 768;

  it("offsets the tooltip down-right of the cursor by default", () => {
    expect(clampTooltip(100, 100, VW, VH)).toEqual({ left: 112, top: 112 });
  });

  it("clamps horizontally at the right edge", () => {
    const { left } = clampTooltip(VW - 10, 100, VW, VH, 320);
    expect(left).toBe(VW - 320);
  });

  it("never pushes left below 0 on a narrow viewport", () => {
    const { left } = clampTooltip(100, 100, 200, VH, 320);
    expect(left).toBe(0);
  });

  it("flips above the cursor near the bottom using the measured height", () => {
    const h = 120;
    const { top } = clampTooltip(100, VH - 20, VW, VH, 320, h);
    // Bottom edge sits TT_FLIP_GAP (8px) above the cursor.
    expect(top).toBe(VH - 20 - h - 8);
  });

  it("never pushes top below 0 when flipping on a short viewport", () => {
    const { top } = clampTooltip(100, 90, 1024, 100, 320, 200);
    expect(top).toBe(0);
  });
});

describe("dedupeTooltipFailures (blank-tooltip regression)", () => {
  it("keeps a per-column rule once per severity, merging its columns", () => {
    const out = dedupeTooltipFailures([
      { rule_name: "not_null", severity: "High", message: "id is null", columns: ["id"] },
      { rule_name: "not_null", severity: "High", message: "id is null", columns: ["name"] },
      { rule_name: "range", severity: "Low", message: "out of range", columns: ["amount"] },
    ]);
    expect(out).toHaveLength(2);
    expect(out[0].columns).toEqual(["id", "name"]);
  });

  it("returns [] for a record with NO parseable failure structs — the live blank-tooltip payload", () => {
    // Observed on the dev workspace (majority of dq_quarantine_records rows):
    // the frozen task runner persisted quarantine rows whose `errors` VARIANT
    // is SQL NULL and whose `warnings` is an empty array, so the backend's
    // parse_failures yields zero structs and FailedRowOut.failures arrives
    // empty. The tooltip must render the "no failure details" fallback for
    // these — never an empty box.
    const records = toFailingRecords([
      {
        record_key: "q-1",
        row_values: { id: "7", name: null },
        failed_columns: [],
        failures: [], // errors NULL + warnings [] -> zero parsed structs
        run_ts: "2026-07-10T00:00:00Z",
      },
    ]);
    expect(dedupeTooltipFailures(records[0].failures)).toEqual([]);
  });

  it("preserves a struct that has a rule name but NO message (fallback line renders)", () => {
    // Second live shape: warnings structs carrying name + user_metadata but no
    // message key (to_json drops null struct fields). The entry must survive
    // dedupe so the tooltip can show the rule name + the "no message" fallback.
    const records = toFailingRecords([
      {
        record_key: "q-2",
        row_values: { id: "8" },
        failed_columns: ["id"],
        failures: [
          { rule_name: "id_not_null", severity: "High", quality_dimension: null, message: null, columns: ["id"] },
        ],
        run_ts: null,
      },
    ]);
    const out = dedupeTooltipFailures(records[0].failures);
    expect(out).toHaveLength(1);
    expect(out[0].rule_name).toBe("id_not_null");
    expect(out[0].message).toBeUndefined();
  });

  it("does not merge distinct severities of the same rule (NUL-separated key)", () => {
    const out = dedupeTooltipFailures([
      { rule_name: "r", severity: "High", columns: ["a"] },
      { rule_name: "r", severity: "Low", columns: ["b"] },
    ]);
    expect(out).toHaveLength(2);
  });

  it("keeps a fully-anonymous failure as ONE entry (renders with both fallbacks)", () => {
    const out = dedupeTooltipFailures([{ columns: [] }, { columns: [] }]);
    expect(out).toHaveLength(1);
    expect(out[0].rule_name).toBeUndefined();
  });
});
