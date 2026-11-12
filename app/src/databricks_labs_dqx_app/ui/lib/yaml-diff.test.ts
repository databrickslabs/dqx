import { describe, expect, test } from "bun:test";
import { alignKeys, diffLines, inlineChange, itemRows, itemStatus, toYaml, unifiedRows } from "./yaml-diff";

describe("diffLines", () => {
  test("pairs a changed line opposite its replacement", () => {
    const rows = diffLines("a\nb\nc", "a\nB\nc");
    expect(rows).toEqual([
      { left: { text: "a", tag: "equal" }, right: { text: "a", tag: "equal" } },
      { left: { text: "b", tag: "removed" }, right: { text: "B", tag: "added" } },
      { left: { text: "c", tag: "equal" }, right: { text: "c", tag: "equal" } },
    ]);
  });

  test("a removed block has nothing opposite it", () => {
    const rows = diffLines("x\ny", "");
    expect(rows.map((r) => [r.left?.tag, r.right])).toEqual([
      ["removed", null],
      ["removed", null],
    ]);
  });
});

describe("unifiedRows", () => {
  test("keeps unchanged lines once and shows a removal before its replacement", () => {
    const cells = unifiedRows(diffLines("a\nb", "a\nc"));
    expect(cells.map((c) => `${c.tag}:${c.text}`)).toEqual(["equal:a", "removed:b", "added:c"]);
  });
});

describe("inlineChange", () => {
  test("marks only the changed middle of a line", () => {
    expect(inlineChange("  criticality: error", "  criticality: warn")).toEqual({
      before: "  criticality: ",
      changed: "error",
      after: "",
    });
    expect(inlineChange("columns: [id, line_no]", "columns: [id, line_number]")).toEqual({
      before: "columns: [id, ",
      changed: "line_no",
      after: "]",
    });
  });

  test("identical lines have nothing marked", () => {
    expect(inlineChange("same", "same").changed).toBe("");
  });
});

describe("toYaml", () => {
  test("renders nothing for a missing value", () => {
    expect(toYaml(null)).toBe("");
  });
});

describe("itemStatus", () => {
  const check = { check: { function: "is_not_null", arguments: { column: "id" } } };
  test("an item missing on one side is added or removed", () => {
    expect(itemStatus([], [check])).toBe("added");
    expect(itemStatus([check], [])).toBe("removed");
  });
  test("equal sides are unchanged, whatever their key order", () => {
    expect(itemStatus([check], [{ check: { arguments: { column: "id" }, function: "is_not_null" } }])).toBe("unchanged");
  });
  test("a changed argument is modified", () => {
    expect(itemStatus([check], [{ check: { function: "is_not_null", arguments: { column: "key" } } }])).toBe("modified");
  });
});

describe("alignKeys", () => {
  test("lays the after side out like the before side, at every level, with new keys last", () => {
    const before = { criticality: "warn", check: { function: "f", arguments: { column: "a" } }, name: "n" };
    const after = { name: "n", extra: 1, check: { arguments: { column: "a" }, function: "f" }, criticality: "error" };
    expect(JSON.stringify(alignKeys(after, before))).toBe(
      JSON.stringify({ criticality: "error", check: { function: "f", arguments: { column: "a" } }, name: "n", extra: 1 }),
    );
  });

  test("aligns array elements by position and leaves scalars alone", () => {
    expect(alignKeys([{ b: 1, a: 2 }, "x"], [{ a: 0, b: 0 }])).toEqual([{ a: 2, b: 1 }, "x"]);
    expect(alignKeys("x", { a: 1 })).toBe("x");
  });
});

describe("itemRows", () => {
  test("the same content in another key order is not a change", () => {
    const rows = itemRows([{ a: 1, b: 2 }], [{ b: 2, a: 1 }]);
    expect(rows.every((r) => r.left?.tag === "equal" && r.right?.tag === "equal")).toBe(true);
  });

  test("only the changed value is a change", () => {
    const rows = itemRows([{ severity: "Low", name: "n" }], [{ name: "n", severity: "High" }]);
    expect(rows.filter((r) => r.left?.tag === "removed").map((r) => r.left?.text)).toEqual(["severity: Low"]);
    expect(rows.filter((r) => r.right?.tag === "added").map((r) => r.right?.text)).toEqual(["severity: High"]);
  });
});
