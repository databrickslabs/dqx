import { describe, expect, it } from "bun:test";
import * as XLSX from "xlsx";
import type { FailingRecord } from "./FailingRecordsTable";
import {
  buildFailedRecordsCsv,
  buildFailedRecordsXlsx,
  columnsFromRows,
  distinctRules,
  EXPORT_ROW_LIMIT,
  failedRecordsFilename,
  toFailingRecords,
} from "./failedRecordsExport";

const rows: FailingRecord[] = [
  {
    record_key: "k1",
    row_values: { id: "1", note: "a, b" },
    failed_columns: ["id"],
    failures: [
      { rule_name: "Not null" },
      { rule_name: "Not null" }, // dupe → collapsed
      { rule_name: "Positive" },
    ],
  },
  {
    record_key: "k2",
    row_values: { id: "2", extra: 'say "hi"' },
    failed_columns: [],
    failures: [],
  },
];

describe("toFailingRecords", () => {
  it("maps the API failed-rows payload, normalising nulls to undefined/[]", () => {
    const out = toFailingRecords([
      {
        record_key: null,
        row_values: undefined,
        failed_columns: undefined,
        failures: [
          {
            rule_name: null,
            severity: "High",
            quality_dimension: null,
            message: "bad",
            columns: undefined,
          },
        ],
      },
    ]);
    expect(out).toEqual([
      {
        record_key: "",
        row_values: {},
        failed_columns: [],
        failures: [
          {
            rule_name: undefined,
            severity: "High",
            quality_dimension: undefined,
            message: "bad",
            columns: [],
          },
        ],
      },
    ]);
  });

  it("returns [] for undefined rows", () => {
    expect(toFailingRecords(undefined)).toEqual([]);
  });
});

describe("columnsFromRows", () => {
  it("returns the union of row_values keys in first-seen order", () => {
    expect(columnsFromRows(rows)).toEqual(["id", "note", "extra"]);
  });

  it("includes a failed column absent from row_values (item 63: null-valued failing column)", () => {
    // The backend renders row_values from `to_json(row_data)`, which omits
    // null fields — so a column that is null on every failing row (e.g. the
    // not-null / is-present check that failed) never appears among the
    // row_values keys. Filtering by it must NOT drop it from the table; the
    // fold over failed_columns keeps it visible (rendered as a "—" cell).
    const nullColRows: FailingRecord[] = [
      {
        record_key: "q1",
        row_values: { order_id: "65", status: "unknown" }, // customer_id is null → absent
        failed_columns: ["customer_id"],
        failures: [{ rule_name: "Value is present", columns: ["customer_id"] }],
      },
    ];
    expect(columnsFromRows(nullColRows)).toEqual([
      "order_id",
      "status",
      "customer_id",
    ]);
  });
});

describe("distinctRules", () => {
  it("dedupes rule names in first-seen order and comma-joins them", () => {
    expect(distinctRules(rows[0])).toBe("Not null, Positive");
  });
  it("drops empty/missing rule names", () => {
    expect(distinctRules(rows[1])).toBe("");
  });
});

describe("buildFailedRecordsCsv", () => {
  it("emits every source column plus a trailing Failed Rules column", () => {
    const csv = buildFailedRecordsCsv(rows);
    const lines = csv.split("\n");
    expect(lines[0]).toBe("id,note,extra,Failed Rules");
    // "a, b" has a comma → quoted; the rules cell is quoted too.
    expect(lines[1]).toBe('1,"a, b",,"Not null, Positive"');
    // Embedded quotes are doubled.
    expect(lines[2]).toBe('2,,"say ""hi""",');
  });
});

describe("buildFailedRecordsXlsx", () => {
  it("round-trips a workbook with sheet 'Failed records' and the CSV's columns", () => {
    const buf = buildFailedRecordsXlsx(rows);
    expect(buf.byteLength).toBeGreaterThan(0);
    const book = XLSX.read(buf, { type: "array" });
    expect(book.SheetNames).toContain("Failed records");
    const aoa = XLSX.utils.sheet_to_json<string[]>(book.Sheets["Failed records"], { header: 1 });
    expect(aoa[0]).toEqual(["id", "note", "extra", "Failed Rules"]);
    expect(aoa[1].slice(0, 2)).toEqual(["1", "a, b"]);
    expect(aoa[1][3]).toBe("Not null, Positive");
  });
});

describe("failedRecordsFilename", () => {
  it("builds <table>_<YYYY-MM-DD>_invalid_records from a friendly name", () => {
    expect(failedRecordsFilename("Customer Orders!")).toMatch(
      /^Customer_Orders_\d{4}-\d{2}-\d{2}_invalid_records$/,
    );
  });
  it("falls back to 'table' for empty/null names", () => {
    expect(failedRecordsFilename(null)).toMatch(/^table_\d{4}-\d{2}-\d{2}_invalid_records$/);
    expect(failedRecordsFilename("  ")).toMatch(/^table_/);
  });
});

describe("EXPORT_ROW_LIMIT", () => {
  it("matches the backend's failed-rows cap", () => {
    expect(EXPORT_ROW_LIMIT).toBe(100000);
  });
});
