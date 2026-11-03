import { describe, expect, it } from "bun:test";
import * as XLSX from "xlsx";
import type { FailingRecord } from "./FailingRecordsTable";
import {
  buildFailedRecordsCsv,
  buildFailedRecordsXlsx,
  columnsFromRows,
  distinctRules,
  collectFailedRowPages,
  EXPORT_ROW_LIMIT,
  FAILED_ROWS_DOWNLOAD_PAGE_SIZE,
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

describe("collectFailedRowPages", () => {
  const row = (key: string) => ({
    record_key: key,
    row_values: { id: key },
    failed_columns: [],
    failures: [{ rule_name: "Value must be unique" }],
  });

  it("pages until the reported total even when a page is shorter than limit", async () => {
    const calls: Array<{ offset: number; limit: number }> = [];
    const pages: Record<number, ReturnType<typeof row>[]> = {
      0: [row("a"), row("b")],
      2: [row("c"), row("d")],
      4: [row("e")],
    };
    const out = await collectFailedRowPages(
      async ({ offset, limit }) => {
        calls.push({ offset, limit });
        return { rows: pages[offset] ?? [], total: 5 };
      },
      { pageSize: 1000 },
    );
    expect(out.map((r) => r.record_key)).toEqual(["a", "b", "c", "d", "e"]);
    expect(calls).toEqual([
      { offset: 0, limit: 1000 },
      { offset: 2, limit: 1000 },
      { offset: 4, limit: 1000 },
    ]);
  });

  it("stops on a short page when total is not larger", async () => {
    const calls: number[] = [];
    const out = await collectFailedRowPages(
      async ({ offset }) => {
        calls.push(offset);
        return { rows: [row("only")], total: 1 };
      },
      { pageSize: 1000 },
    );
    expect(out).toHaveLength(1);
    expect(calls).toEqual([0]);
  });

  it("keeps paging while a page is full when total is unknown", async () => {
    const calls: number[] = [];
    const out = await collectFailedRowPages(
      async ({ offset, limit }) => {
        calls.push(offset);
        if (offset === 0) return { rows: [row("a"), row("b")], total: null, run_id: "run-1" };
        return { rows: [row("c")], total: null, run_id: "run-1" };
      },
      { pageSize: 2 },
    );
    expect(out.map((r) => r.record_key)).toEqual(["a", "b", "c"]);
    expect(calls).toEqual([0, 2]);
  });

  it("pins run_id from the first page onto later requests", async () => {
    const runIds: Array<string | undefined> = [];
    await collectFailedRowPages(
      async ({ offset, limit, run_id }) => {
        runIds.push(run_id);
        if (offset === 0) return { rows: [row("a"), row("b")], total: 3, run_id: "run-9" };
        return { rows: [row("c")], total: 3, run_id: "run-9" };
      },
      { pageSize: 2 },
    );
    expect(runIds).toEqual([undefined, "run-9"]);
  });

  it("requests 5000-row pages by default", () => {
    expect(FAILED_ROWS_DOWNLOAD_PAGE_SIZE).toBe(5000);
  });
});
