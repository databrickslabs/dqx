import { describe, expect, it } from "bun:test";
import { includeDraftsParam } from "./RunModeSelect";
import { EMPTY_FILTERS } from "@/components/monitored-tables/BindingResultsTab";
import { breakdownParams } from "./MultiTableResults";

describe("includeDraftsParam", () => {
  it("is true ONLY when the dropdown includes drafts", () => {
    expect(includeDraftsParam(true)).toBe(true);
  });

  it("is undefined (param omitted → backend published-only default) otherwise", () => {
    expect(includeDraftsParam(false)).toBeUndefined();
  });
});

describe("breakdownParams run-mode wiring", () => {
  it("omits include_drafts in published-only mode", () => {
    expect(breakdownParams(EMPTY_FILTERS).include_drafts).toBeUndefined();
    expect(breakdownParams(EMPTY_FILTERS, false).include_drafts).toBeUndefined();
  });

  it("passes include_drafts: true in Published + Draft mode", () => {
    expect(breakdownParams(EMPTY_FILTERS, true).include_drafts).toBe(true);
  });
});
