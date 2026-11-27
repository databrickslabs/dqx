import { describe, expect, it } from "bun:test";
import { AxiosError, AxiosHeaders, type InternalAxiosRequestConfig } from "axios";
import type { FailedRowOut, FailedRowsOut } from "@/lib/api";
import {
  collectFailedRecords,
  downloadRunId,
  EXPORT_LIMITS,
  outcomeNotices,
  runFailedRecordsDownload,
  type CollectFailedRecordsInput,
  type ExportProgress,
  type FetchFailedRowsPage,
  type RunFailedRecordsDownloadInput,
} from "./failedRecordsDownload";
import { EXCEL_CELL_MAX_CHARS } from "./failedRecordsExport";

const RUN = "run-1";

const row = (id: string, values: Record<string, string> = {}): FailedRowOut => ({
  record_key: id,
  row_values: { id, ...values },
  failed_columns: [],
  failures: [{ rule_name: "Not null" }],
});

const page = (
  ids: string[],
  next: string | null,
  extra: Partial<FailedRowsOut> = {},
): FailedRowsOut => ({
  rows: ids.map((id) => row(id)),
  total: ids.length,
  suppressed: false,
  run_id: RUN,
  next_cursor: next,
  ...extra,
});

const httpError = (status: number, data: unknown = {}) =>
  new AxiosError("request failed", "ERR", undefined, undefined, {
    status,
    statusText: "",
    data,
    headers: {},
    config: { headers: new AxiosHeaders() } as InternalAxiosRequestConfig,
  });

function harness(steps: Array<FailedRowsOut | { throws: unknown }>) {
  const calls: Array<{ runId?: string; after?: string; limit: number }> = [];
  const fetchPage: FetchFailedRowsPage = async ({ runId, after, limit }) => {
    calls.push({ runId, after, limit });
    const step = steps.shift();
    if (step === undefined) throw new Error("unexpected request");
    if ("throws" in step) throw step.throws;
    return step;
  };
  return { calls, fetchPage };
}

type Harness = ReturnType<typeof harness>;

const collect = (h: Harness, input: Partial<CollectFailedRecordsInput> = {}) =>
  collectFailedRecords({
    fetchPage: h.fetchPage,
    format: "csv",
    signal: new AbortController().signal,
    ...input,
    limits: { retryDelaysMs: [0, 0], ...input.limits },
  });

const ids = (rows: Array<{ record_key: string }>) => rows.map((r) => r.record_key);

describe("collectFailedRecords", () => {
  it.each([["run-caller"], [undefined]])(
    "follows next_cursor and pins the run (caller run %p)",
    async (callerRunId) => {
      const h = harness([
        page(["a", "b"], "c1", { total: 5 }),
        page(["c", "d"], "c2"),
        page(["e"], null),
      ]);
      const progress: ExportProgress[] = [];
      const result = await collect(h, {
        runId: callerRunId,
        onProgress: (p) => progress.push(p),
      });

      expect(h.calls).toEqual([
        { runId: callerRunId, after: undefined, limit: 5000 },
        { runId: RUN, after: "c1", limit: 5000 },
        { runId: RUN, after: "c2", limit: 5000 },
      ]);
      expect(ids(result.rows)).toEqual(["a", "b", "c", "d", "e"]);
      expect(result.capped).toBeNull();
      expect(progress).toEqual([
        { loaded: 2, total: 5 },
        { loaded: 4, total: 5 },
        { loaded: 5, total: null },
      ]);
    },
  );

  it("stops at maxRows when more rows follow and asks only for the remaining rows", async () => {
    const h = harness([page(["a", "b"], "c1"), page(["c", "d"], "c2"), page(["e"], "c3")]);
    const result = await collect(h, { limits: { pageRows: 2, maxRows: 5 } });
    expect(h.calls.map((c) => c.limit)).toEqual([2, 2, 1]);
    expect(result.rows).toHaveLength(5);
    expect(result.capped).toBe("rows");
  });

  it("does not cap a run that ends exactly at maxRows", async () => {
    const h = harness([page(["a", "b"], "c1"), page(["c", "d"], "c2"), page(["e"], null)]);
    const result = await collect(h, { limits: { pageRows: 2, maxRows: 5 } });
    expect(result.rows).toHaveLength(5);
    expect(result.capped).toBeNull();
  });

  it("stops with capped 'size' once the row text reaches maxChars", async () => {
    // Each row's only value is its 6-character id.
    const h = harness([page(["aaaaaa"], "c1"), page(["bbbbbb"], "c2"), page(["cccccc"], null)]);
    const result = await collect(h, { limits: { maxChars: 10 } });
    expect(h.calls).toHaveLength(2);
    expect(ids(result.rows)).toEqual(["aaaaaa", "bbbbbb"]);
    expect(result.capped).toBe("size");
  });

  it("counts the columns a row adds against the Excel cell cap", async () => {
    // to_json drops null fields, so later rows can widen the sheet.
    const rows = [row("a"), row("b", { v1: "1", v2: "2", v3: "3" })];
    const result = await collect(harness([page([], null, { rows })]), {
      format: "xlsx",
      limits: { maxXlsxCells: 10 },
    });
    // Row a alone is 2 x 2 = 4 cells; row b would make it 3 x 5 = 15.
    expect(ids(result.rows)).toEqual(["a"]);
    expect(result.capped).toBe("cells");
  });

  it("ignores the cell cap for CSV", async () => {
    const rows = ["a", "b", "c"].map((id) => row(id, { v: "x" }));
    const result = await collect(harness([page([], null, { rows })]), {
      limits: { maxXlsxCells: 10 },
    });
    expect(result.rows).toHaveLength(3);
    expect(result.capped).toBeNull();
  });

  it.each([
    ["no rows", page([], null)],
    ["suppressed", page(["x"], null, { suppressed: true })],
    ["another run", page(["x"], null, { run_id: "run-2" })],
  ])("retries a contradictory continuation page (%s) with the same cursor", async (_, bad) => {
    const h = harness([page(["a"], "c1"), bad, page(["b"], null)]);
    const result = await collect(h);
    expect(h.calls.map((c) => c.after)).toEqual([undefined, "c1", "c1"]);
    expect(ids(result.rows)).toEqual(["a", "b"]);
  });

  it("rejects once a contradictory page outlasts the retries", async () => {
    const bad = page([], null);
    const h = harness([page(["a"], "c1"), bad, bad, bad]);
    await expect(collect(h)).rejects.toThrow("continuation page");
    expect(h.calls.map((c) => c.after)).toEqual([undefined, "c1", "c1", "c1"]);
  });

  it.each([
    ["HTTP 503", httpError(503)],
    ["HTTP 429", httpError(429)],
    ["no response", new Error("Network Error")],
  ])("retries a transient failure (%s)", async (_, error) => {
    const h = harness([{ throws: error }, page(["a"], null)]);
    const result = await collect(h);
    expect(h.calls).toHaveLength(2);
    expect(ids(result.rows)).toEqual(["a"]);
  });

  it("does not retry a client error", async () => {
    const error = httpError(422);
    const h = harness([{ throws: error }, page(["a"], null)]);
    await expect(collect(h)).rejects.toBe(error);
    expect(h.calls).toHaveLength(1);
  });

  it("fails without retrying when a page announces more rows but names no run", async () => {
    const h = harness([page(["a"], "c1", { run_id: null })]);
    await expect(collect(h)).rejects.toBeInstanceOf(Error);
    expect(h.calls).toHaveLength(1);
  });
});

async function download(
  steps: Parameters<typeof harness>[0],
  input: Partial<RunFailedRecordsDownloadInput> = {},
) {
  const saved: Array<{ data: BlobPart; filename: string; mime: string }> = [];
  const errors: unknown[] = [];
  const outcome = await runFailedRecordsDownload({
    fetchPage: harness(steps).fetchPage,
    format: "csv",
    signal: new AbortController().signal,
    filenameBase: "orders",
    save: (data, filename, mime) => saved.push({ data, filename, mime }),
    reportError: (error) => errors.push(error),
    ...input,
    limits: { retryDelaysMs: [0, 0], ...input.limits },
  });
  return { outcome, saved, errors };
}

describe("runFailedRecordsDownload", () => {
  it("resolves 'failed' with only the localized notice when the fetch fails", async () => {
    const error = httpError(400, { detail: "SELECT * FROM cat.sch.secret" });
    const { outcome, saved, errors } = await download([{ throws: error }]);
    expect(outcome).toEqual({ status: "failed" });
    expect(saved).toEqual([]);
    expect(errors).toEqual([error]);
    expect(outcomeNotices(outcome)).toEqual([{ level: "error", key: "resultsUi.downloadFailed" }]);
  });

  it("resolves 'failed' with only the localized notice when saving the file throws", async () => {
    const error = new Error("disk full");
    const { outcome, errors } = await download([page(["a"], null)], {
      save: () => {
        throw error;
      },
    });
    expect(outcome).toEqual({ status: "failed" });
    expect(errors).toEqual([error]);
    expect(outcomeNotices(outcome)).toEqual([{ level: "error", key: "resultsUi.downloadFailed" }]);
  });

  it("reports an empty run without saving", async () => {
    const { outcome, saved } = await download([page([], null)]);
    expect(outcome).toEqual({ status: "empty" });
    expect(saved).toEqual([]);
    expect(outcomeNotices(outcome)).toEqual([{ level: "info", key: "resultsUi.downloadNothing" }]);
  });

  it("reports a cancel without saving, notices or an error report", async () => {
    const controller = new AbortController();
    controller.abort();
    const { outcome, saved, errors } = await download([], { signal: controller.signal });
    expect(outcome).toEqual({ status: "cancelled" });
    expect(saved).toEqual([]);
    expect(errors).toEqual([]);
    expect(outcomeNotices(outcome)).toEqual([]);
  });

  it.each([
    ["the record cap", { maxRows: 1 }],
    ["the size cap", { maxChars: 1 }],
  ])("names a file stopped by %s _first_<N> and warns about the cap", async (_, limits) => {
    const { outcome, saved } = await download([page(["a"], "c1")], { limits });
    expect(outcome).toMatchObject({ status: "saved", rowCount: 1 });
    expect(saved.map((f) => f.filename)).toEqual(["orders_first_1.csv"]);
    expect(outcomeNotices(outcome)).toEqual([
      { level: "warning", key: "resultsUi.downloadCapped", params: { loaded: "1" } },
    ]);
  });

  it("warns about the Excel cell cap when it trims the sheet", async () => {
    // id + Failed Rules = 2 columns; 4 rows + header = 10 cells fit in 11.
    const { outcome, saved } = await download([page(["a", "b", "c", "d", "e"], null)], {
      format: "xlsx",
      limits: { maxXlsxCells: 11 },
    });
    expect(outcome).toMatchObject({ status: "saved", rowCount: 4, capped: "cells" });
    expect(saved[0].filename).toBe("orders_first_4.xlsx");
    expect(outcomeNotices(outcome)).toEqual([
      {
        level: "warning",
        key: "resultsUi.downloadCappedExcel",
        params: { loaded: "4", cells: EXPORT_LIMITS.maxXlsxCells.toLocaleString() },
      },
    ]);
  });

  it("warns when an Excel cell had to be shortened", async () => {
    const rows = [row("a", { payload: "z".repeat(40_000) })];
    const { outcome, saved } = await download([page([], null, { rows })], { format: "xlsx" });
    expect(outcome).toEqual({ status: "saved", rowCount: 1, capped: null, truncatedCells: 1 });
    expect(saved[0].filename).toBe("orders.xlsx");
    expect(saved[0].mime).toBe("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    expect(outcomeNotices(outcome)).toEqual([
      {
        level: "warning",
        key: "resultsUi.downloadExcelCellsTruncated",
        params: { count: 1, limit: EXCEL_CELL_MAX_CHARS.toLocaleString() },
      },
    ]);
  });

  it("saves a complete run once with no notices", async () => {
    const { outcome, saved } = await download([page(["a"], "c1"), page(["b"], null)]);
    expect(outcome).toEqual({ status: "saved", rowCount: 2, capped: null, truncatedCells: 0 });
    expect(saved).toEqual([
      {
        data: "id,Failed Rules\na,Not null\nb,Not null",
        filename: "orders.csv",
        mime: "text/csv;charset=utf-8;",
      },
    ]);
    expect(outcomeNotices(outcome)).toEqual([]);
  });
});

describe("downloadRunId", () => {
  it("uses the run the user picked, whatever the table shows", () => {
    expect(
      downloadRunId({ pickedRunId: "r-old", shownRunId: "r-new", isPlaceholderData: false }),
    ).toBe("r-old");
    expect(
      downloadRunId({ pickedRunId: "r-old", shownRunId: "r-new", isPlaceholderData: true }),
    ).toBe("r-old");
  });

  it("pins the run on screen when none was picked, so a newer run cannot replace it", () => {
    expect(downloadRunId({ shownRunId: "r-shown", isPlaceholderData: false })).toBe("r-shown");
  });

  it("never pins placeholder data, which belongs to the previous table or run", () => {
    expect(downloadRunId({ shownRunId: "r-previous", isPlaceholderData: true })).toBeUndefined();
  });

  it("leaves the run to the endpoint when the table shows no rows", () => {
    expect(downloadRunId({ shownRunId: null, isPlaceholderData: false })).toBeUndefined();
  });
});
