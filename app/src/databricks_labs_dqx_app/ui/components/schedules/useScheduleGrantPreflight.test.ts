import { describe, expect, it } from "bun:test";
import { splitPreflightTables } from "./useScheduleGrantPreflight";

describe("splitPreflightTables", () => {
  it("separates denied tables from unverified ones", () => {
    const result = splitPreflightTables([
      { fqn: "c.s.ok", can_manage: true },
      {
        fqn: "c.s.denied",
        can_manage: false,
        manage_holders: [{ principal: "bob", type: "user" }],
      },
      { fqn: "c.s.unknown", can_manage: false, access_unverified: true },
    ]);
    expect(result.blockedTables.map((t) => t.fqn)).toEqual(["c.s.denied"]);
    expect(result.unverifiedTables.map((t) => t.fqn)).toEqual(["c.s.unknown"]);
    expect(result.hasGrantIssue).toBe(true);
  });

  it("an unverified table alone still blocks the save", () => {
    const result = splitPreflightTables([
      { fqn: "c.s.unknown", can_manage: false, access_unverified: true },
    ]);
    expect(result.blockedTables).toEqual([]);
    expect(result.hasGrantIssue).toBe(true);
  });

  it("no issue when every table is grantable", () => {
    expect(
      splitPreflightTables([{ fqn: "c.s.ok", can_manage: true }]).hasGrantIssue,
    ).toBe(false);
  });
});
