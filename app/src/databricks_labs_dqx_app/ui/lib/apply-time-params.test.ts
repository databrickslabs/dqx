import { describe, expect, test } from "bun:test";
import {
  bindsEveryParameter,
  coerceParameterValue,
  inferSqlParamType,
  isSqlParamPlaceholderName,
  partitionPlaceholderSlots,
  mappingGroupKey,
  mustBindParameter,
  parameterInputFamily,
  validateGroupParameterValues,
} from "./apply-time-params";

describe("apply-time parameter validation", () => {
  test("maximum length must be a positive whole number", () => {
    expect(coerceParameterValue({ name: "column_max_length", type: "number" }, "25")).toBe(25);
    expect(() => coerceParameterValue({ name: "column_max_length", type: "number" }, "")).toThrow(
      "column_max_length is required",
    );
    expect(() => coerceParameterValue({ name: "column_max_length", type: "number" }, "0")).toThrow(
      "positive whole number",
    );
    expect(() =>
      coerceParameterValue({ name: "column_max_length", type: "number" }, "{{column_max_length}}"),
    ).toThrow("required");
  });

  test("range minimum cannot exceed maximum", () => {
    const params = [
      { name: "min_limit", type: "number" as const, value: null },
      { name: "max_limit", type: "number" as const, value: null },
    ];
    expect(validateGroupParameterValues(params, { min_limit: 1, max_limit: 5 }).error).toBeNull();
    expect(
      validateGroupParameterValues(params, { min_limit: 9, max_limit: 2 }).error,
    ).toMatch(/must not be greater/);
  });

  test("mapping group keys distinguish columns of the same rule", () => {
    expect(mappingGroupKey({ column: "email" })).not.toBe(mappingGroupKey({ column: "name" }));
  });

  test("SQL max-length tokens are parameters, not columns", () => {
    expect(isSqlParamPlaceholderName("column_max_length")).toBe(true);
    expect(isSqlParamPlaceholderName("limit")).toBe(true);
    expect(isSqlParamPlaceholderName("threshold")).toBe(true);
    expect(isSqlParamPlaceholderName("column")).toBe(false);
  });

  test("partitionPlaceholderSlots splits columns from apply-time values", () => {
    const { columns, parameters } = partitionPlaceholderSlots([
      { name: "email", family: "text" },
      { name: "max_length", family: "any" },
      { name: "limit", family: "any" },
    ]);
    expect(columns.map((c) => c.name)).toEqual(["email"]);
    expect(parameters.map((p) => p.name)).toEqual(["max_length", "limit"]);
  });
});

describe("typed parameters", () => {
  test("integer must be a whole number", () => {
    expect(coerceParameterValue({ name: "threshold", type: "integer" }, "7")).toBe(7);
    expect(coerceParameterValue({ name: "threshold", type: "integer" }, "-3")).toBe(-3);
    expect(() => coerceParameterValue({ name: "threshold", type: "integer" }, "1.5")).toThrow(
      "threshold must be a whole number.",
    );
  });

  test("scale-named integers may be zero but not negative", () => {
    expect(coerceParameterValue({ name: "allowed_scale", type: "integer" }, "0")).toBe(0);
    expect(() => coerceParameterValue({ name: "allowed_scale", type: "integer" }, "-1")).toThrow(
      "zero or a positive whole number",
    );
  });

  test("decimal accepts fractions", () => {
    expect(coerceParameterValue({ name: "tolerance", type: "decimal" }, "0.25")).toBe(0.25);
    expect(() => coerceParameterValue({ name: "tolerance", type: "decimal" }, "abc")).toThrow(
      "tolerance must be a number.",
    );
  });

  test("date must be a real ISO calendar date", () => {
    expect(coerceParameterValue({ name: "start_date", type: "date" }, "2026-02-28")).toBe("2026-02-28");
    expect(coerceParameterValue({ name: "start_date", type: "date" }, "2026-02-28T10:00:00")).toBe("2026-02-28");
    expect(() => coerceParameterValue({ name: "start_date", type: "date" }, "2026-02-30")).toThrow(
      "start_date must be a date (YYYY-MM-DD).",
    );
    expect(() => coerceParameterValue({ name: "start_date", type: "date" }, "28/02/2026")).toThrow("YYYY-MM-DD");
  });

  test("SQL and low-code rules bind every parameter", () => {
    expect(bindsEveryParameter("sql")).toBe(true);
    expect(bindsEveryParameter("lowcode")).toBe(true);
    expect(bindsEveryParameter("dqx_native")).toBe(false);
    const label = { name: "label", type: "string" as const, value: null };
    expect(mustBindParameter(label)).toBe(false);
    expect(mustBindParameter(label, true)).toBe(true);
    expect(validateGroupParameterValues([label], {}).error).toBeNull();
    expect(validateGroupParameterValues([label], {}, true).error).toBe("label is required.");
  });

  test("typed numbers and dates are always bound", () => {
    expect(mustBindParameter({ name: "cutoff", type: "date", value: null })).toBe(true);
    expect(mustBindParameter({ name: "factor", type: "decimal", value: null })).toBe(true);
    expect(mustBindParameter({ name: "count", type: "integer", value: null })).toBe(true);
  });

  test("placeholder names infer a type", () => {
    expect(inferSqlParamType("column_max_length")).toBe("integer");
    expect(inferSqlParamType("allowed_scale")).toBe("integer");
    expect(inferSqlParamType("max_value")).toBe("decimal");
    expect(inferSqlParamType("reference_date")).toBe("date");
    expect(inferSqlParamType("allowed")).toBe("list");
    expect(inferSqlParamType("pattern")).toBe("regex");
    expect(inferSqlParamType("label")).toBe("string");
  });

  test("each type maps to a Test-rule input family", () => {
    expect(parameterInputFamily("integer")).toBe("numeric");
    expect(parameterInputFamily("decimal")).toBe("numeric");
    expect(parameterInputFamily("number")).toBe("numeric");
    expect(parameterInputFamily("date")).toBe("temporal");
    expect(parameterInputFamily("boolean")).toBe("boolean");
    expect(parameterInputFamily("list")).toBe("text");
  });
});
