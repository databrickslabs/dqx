import { describe, expect, test } from "bun:test";
import {
  isFqnLikeTableSearch,
  matchesTableFqnSearch,
  normalizeTableSearchQuery,
} from "./monitored-tables-search";

describe("normalizeTableSearchQuery", () => {
  test("strips backticks and whitespace around dots", () => {
    expect(normalizeTableSearchQuery("  `samples` . `bakehouse` . `sales_transactions`  ")).toBe(
      "samples.bakehouse.sales_transactions",
    );
  });

  test("strips quotes", () => {
    expect(normalizeTableSearchQuery('"samples.bakehouse.sales"')).toBe("samples.bakehouse.sales");
  });
});

describe("matchesTableFqnSearch", () => {
  const fqn = "samples.bakehouse.sales_transactions";

  test("empty query matches everything", () => {
    expect(matchesTableFqnSearch(fqn, "")).toBe(true);
    expect(matchesTableFqnSearch(fqn, "   ")).toBe(true);
  });

  test("matches bare table name substring", () => {
    expect(matchesTableFqnSearch(fqn, "sales")).toBe(true);
    expect(matchesTableFqnSearch(fqn, "transactions")).toBe(true);
    expect(matchesTableFqnSearch(fqn, "nope")).toBe(false);
  });

  test("matches full catalog.schema.table", () => {
    expect(matchesTableFqnSearch(fqn, "samples.bakehouse.sales_transactions")).toBe(true);
    expect(matchesTableFqnSearch(fqn, "SAMPLES.Bakehouse.Sales_Transactions")).toBe(true);
  });

  test("matches backticked pasted FQN", () => {
    expect(matchesTableFqnSearch(fqn, "`samples`.`bakehouse`.`sales_transactions`")).toBe(true);
  });

  test("matches catalog.schema prefix", () => {
    expect(matchesTableFqnSearch(fqn, "samples.bakehouse")).toBe(true);
    expect(matchesTableFqnSearch(fqn, "samples.other")).toBe(false);
  });

  test("matches schema.table suffix", () => {
    expect(matchesTableFqnSearch(fqn, "bakehouse.sales_transactions")).toBe(true);
    expect(matchesTableFqnSearch(fqn, "bakehouse.sales")).toBe(true);
  });

  test("partial segment prefixes", () => {
    expect(matchesTableFqnSearch(fqn, "samp.bake.sales")).toBe(true);
  });
});

describe("isFqnLikeTableSearch", () => {
  test("true when query contains a dot", () => {
    expect(isFqnLikeTableSearch("a.b")).toBe(true);
    expect(isFqnLikeTableSearch("sales")).toBe(false);
  });
});
