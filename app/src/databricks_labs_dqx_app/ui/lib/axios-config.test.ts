import { describe, expect, it } from "bun:test";
import { axios } from "./axios-config";

/**
 * Regression tests for the drilldown cross-filtering bug: axios's DEFAULT
 * array-param serialization emits bracketed keys (`dimension[]=a`), which
 * FastAPI's `Annotated[list[str] | None, Query()]` parameters do NOT parse
 * (they collect repeated bare keys only) — so every facet filter arrived as
 * None server-side and clicking a breakdown row never filtered the other
 * boxes. `lib/axios-config.ts` sets `paramsSerializer = { indexes: null }`
 * so arrays serialize as repeated keys, matching dqlake's hand-rolled
 * client (`searchParams.append(...)` per value).
 *
 * `axios.getUri` honours `axios.defaults.paramsSerializer`, so these tests
 * exercise the exact serialization every generated hook uses.
 */
describe("axios array-param serialization (FastAPI repeated-key contract)", () => {
  it("serializes a multi-value facet as repeated bare keys", () => {
    const uri = axios.getUri({
      url: "/api/v1/dq-results/table/c.s.t",
      params: { dimension: ["Completeness", "Validity"] },
    });
    expect(uri).toBe(
      "/api/v1/dq-results/table/c.s.t?dimension=Completeness&dimension=Validity",
    );
  });

  it("serializes a single-value facet without brackets", () => {
    const uri = axios.getUri({
      url: "/x",
      params: { severity: ["Critical"] },
    });
    expect(uri).toBe("/x?severity=Critical");
  });

  it("never emits bracketed keys for any facet", () => {
    const uri = axios.getUri({
      url: "/x",
      params: {
        dimension: ["a"],
        severity: ["b", "c"],
        rule: ["r 1"],
        column: ["col"],
        axes: "breakdown",
      },
    });
    expect(uri).not.toContain("%5B%5D"); // encoded "[]"
    expect(uri).not.toContain("[]");
    expect(uri).toContain("severity=b&severity=c");
    expect(uri).toContain("axes=breakdown");
  });

  it("omits undefined params (empty facets are dropped by facetQueryParams)", () => {
    const uri = axios.getUri({
      url: "/x",
      params: { dimension: undefined, axes: "trend" },
    });
    expect(uri).toBe("/x?axes=trend");
  });

  it("URL-encodes values containing reserved characters", () => {
    const uri = axios.getUri({
      url: "/x",
      params: { rule: ["a&b=c"] },
    });
    expect(uri).toBe("/x?rule=a%26b%3Dc");
  });
});
