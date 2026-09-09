import { describe, expect, it } from "bun:test";
import { expandJsonColumns } from "./GenieResultTable";

// Data-level ports of dqlake's GenieResultTable DOM tests: the expansion of a
// JSON-object column (e.g. `failing_record`) into the record's own columns.

describe("expandJsonColumns", () => {
  it("expands a JSON-object column into the record's own columns", () => {
    const { columns, rows } = expandJsonColumns(
      ["failing_record"],
      [
        ['{"customer_id":"CUST-01","email":"@gmail.com","city":"Lake Anthony"}'],
        ['{"customer_id":"CUST-02","email":"a@b","city":"South Steven"}'],
      ],
    );
    expect(columns).toEqual(["customer_id", "email", "city"]);
    expect(rows[0]).toEqual(["CUST-01", "@gmail.com", "Lake Anthony"]);
  });

  it("keeps sibling columns in place when expanding", () => {
    const { columns, rows } = expandJsonColumns(
      ["failing_record", "rules_failed"],
      [['{"customer_id":"CUST-09","email":"@x.com"}', "3"]],
    );
    expect(columns).toEqual(["customer_id", "email", "rules_failed"]);
    expect(rows[0]).toEqual(["CUST-09", "@x.com", "3"]);
  });

  it("leaves a plain (non-JSON) result untouched", () => {
    const columns = ["rule_name", "failed_tests"];
    const rows = [
      ["Email Format Valid", "408"],
      ["Account Tier Valid", "0"],
    ];
    const out = expandJsonColumns(columns, rows);
    expect(out.columns).toBe(columns);
    expect(out.rows).toBe(rows);
  });

  it("unions keys (first-seen order) and fills missing keys with null", () => {
    const { columns, rows } = expandJsonColumns(
      ["failing_record"],
      [['{"a":"1","b":"2"}'], ['{"a":"3"}']],
    );
    expect(columns).toEqual(["a", "b"]);
    expect(rows[1]).toEqual(["3", null]);
  });

  it("does not treat arrays, scalars, or mixed columns as JSON records", () => {
    const mixed = expandJsonColumns(
      ["cell"],
      [['{"a":1}'], ["plain"]], // one JSON, one not — column must not expand
    );
    expect(mixed.columns).toEqual(["cell"]);
    const arrays = expandJsonColumns(["cell"], [["[1,2]"], ["[3]"]]);
    expect(arrays.columns).toEqual(["cell"]);
  });

  it("stringifies non-string JSON values", () => {
    const { rows } = expandJsonColumns(
      ["failing_record"],
      [['{"n":42,"b":true,"x":null}'], ['{"n":1,"b":false,"x":"y"}']],
    );
    expect(rows[0]).toEqual(["42", "true", null]);
  });
});
