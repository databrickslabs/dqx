import { describe, expect, test } from "bun:test";

import {
  MAX_VISIBLE_OPTIONS,
  optionMatches,
  setupOptionList,
} from "./setup-options";

describe("setupOptionList", () => {
  test("is loading while the catalog list is pending", () => {
    expect(setupOptionList({ isPending: true, isError: false })).toEqual({
      status: "loading",
    });
  });

  test("offers the listed catalogs once they load", () => {
    expect(
      setupOptionList({
        isPending: false,
        isError: false,
        options: ["main", "sales"],
      }),
    ).toEqual({ status: "ready", options: ["main", "sales"], truncated: false });
  });

  test("says when the listing was cut short", () => {
    expect(
      setupOptionList({
        isPending: false,
        isError: false,
        options: ["data-team"],
        truncated: true,
      }),
    ).toEqual({ status: "ready", options: ["data-team"], truncated: true });
  });

  test("falls back to typing when listing fails", () => {
    expect(setupOptionList({ isPending: false, isError: true })).toEqual({
      status: "unavailable",
    });
  });

  test("falls back to typing when no catalogs are visible", () => {
    expect(
      setupOptionList({ isPending: false, isError: false, options: [] }),
    ).toEqual({ status: "unavailable" });
  });
});

describe("optionMatches", () => {
  const catalogs = ["main", "sales_dev", "sales_prod", "hr"];

  test("lists every catalog when nothing is typed", () => {
    expect(optionMatches(catalogs, "")).toEqual({
      matches: catalogs,
      typed: null,
    });
  });

  test("filters by a case-insensitive substring", () => {
    expect(optionMatches(catalogs, " SALES ").matches).toEqual([
      "sales_dev",
      "sales_prod",
    ]);
  });

  test("offers a typed name that is not listed", () => {
    expect(optionMatches(catalogs, " finance ")).toEqual({
      matches: [],
      typed: "finance",
    });
  });

  test("offers a partial typed name alongside its matches", () => {
    expect(optionMatches(catalogs, "sales")).toEqual({
      matches: ["sales_dev", "sales_prod"],
      typed: "sales",
    });
  });

  test("does not offer a typed name that matches a listed catalog", () => {
    expect(optionMatches(catalogs, "MAIN").typed).toBeNull();
  });

  test("keeps a huge list searchable by capping what is shown", () => {
    const many = Array.from({ length: 5000 }, (_, index) => `catalog_${index}`);

    expect(optionMatches(many, "").matches).toHaveLength(MAX_VISIBLE_OPTIONS);
    expect(optionMatches(many, "catalog_4999").matches).toEqual([
      "catalog_4999",
    ]);
  });
});
