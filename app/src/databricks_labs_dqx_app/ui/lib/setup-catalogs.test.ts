import { describe, expect, test } from "bun:test";

import {
  MAX_VISIBLE_CATALOGS,
  catalogMatches,
  setupCatalogList,
} from "./setup-catalogs";

describe("setupCatalogList", () => {
  test("is loading while the catalog list is pending", () => {
    expect(setupCatalogList({ isPending: true, isError: false })).toEqual({
      status: "loading",
    });
  });

  test("offers the listed catalogs once they load", () => {
    expect(
      setupCatalogList({
        isPending: false,
        isError: false,
        catalogs: ["main", "sales"],
      }),
    ).toEqual({ status: "ready", catalogs: ["main", "sales"] });
  });

  test("falls back to typing when listing fails", () => {
    expect(setupCatalogList({ isPending: false, isError: true })).toEqual({
      status: "unavailable",
    });
  });

  test("falls back to typing when no catalogs are visible", () => {
    expect(
      setupCatalogList({ isPending: false, isError: false, catalogs: [] }),
    ).toEqual({ status: "unavailable" });
  });
});

describe("catalogMatches", () => {
  const catalogs = ["main", "sales_dev", "sales_prod", "hr"];

  test("lists every catalog when nothing is typed", () => {
    expect(catalogMatches(catalogs, "")).toEqual({
      matches: catalogs,
      typed: null,
    });
  });

  test("filters by a case-insensitive substring", () => {
    expect(catalogMatches(catalogs, " SALES ").matches).toEqual([
      "sales_dev",
      "sales_prod",
    ]);
  });

  test("offers a typed name that is not listed", () => {
    expect(catalogMatches(catalogs, " finance ")).toEqual({
      matches: [],
      typed: "finance",
    });
  });

  test("offers a partial typed name alongside its matches", () => {
    expect(catalogMatches(catalogs, "sales")).toEqual({
      matches: ["sales_dev", "sales_prod"],
      typed: "sales",
    });
  });

  test("does not offer a typed name that matches a listed catalog", () => {
    expect(catalogMatches(catalogs, "MAIN").typed).toBeNull();
  });

  test("keeps a huge list searchable by capping what is shown", () => {
    const many = Array.from({ length: 5000 }, (_, index) => `catalog_${index}`);

    expect(catalogMatches(many, "").matches).toHaveLength(MAX_VISIBLE_CATALOGS);
    expect(catalogMatches(many, "catalog_4999").matches).toEqual([
      "catalog_4999",
    ]);
  });
});
