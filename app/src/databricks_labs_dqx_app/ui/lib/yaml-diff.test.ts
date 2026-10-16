import { describe, expect, test } from "bun:test";
import { diffLines, inlineChange, itemStatus, toYaml, unifiedRows } from "./yaml-diff";

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
