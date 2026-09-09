import { describe, expect, it } from "vitest";
import { parsePastedTableFqns } from "./parsePastedTableFqns";

describe("parsePastedTableFqns", () => {
  it("parses a single catalog.schema.table", () => {
    expect(parsePastedTableFqns("samples.bakehouse.sales_customers")).toEqual({
      valid: ["samples.bakehouse.sales_customers"],
      invalid: [],
    });
  });

  it("accepts multiple FQNs separated by newlines, commas, or semicolons", () => {
    expect(
      parsePastedTableFqns(
        "cat.sch.t1\ncat.sch.t2, cat.sch.t3;cat.sch.t4",
      ),
    ).toEqual({
      valid: ["cat.sch.t1", "cat.sch.t2", "cat.sch.t3", "cat.sch.t4"],
      invalid: [],
    });
  });

  it("strips surrounding backticks and dedupes", () => {
    expect(parsePastedTableFqns("`cat.sch.tbl`, cat.sch.tbl")).toEqual({
      valid: ["cat.sch.tbl"],
      invalid: [],
    });
  });

  it("reports incomplete tokens as invalid", () => {
    expect(parsePastedTableFqns("cat.sch\ncat.sch.tbl.extra\ncat..tbl")).toEqual({
      valid: [],
      invalid: ["cat.sch", "cat.sch.tbl.extra", "cat..tbl"],
    });
  });

  it("returns empty lists for blank input", () => {
    expect(parsePastedTableFqns("  \n  ")).toEqual({ valid: [], invalid: [] });
  });
});
