import { describe, expect, test } from "bun:test";
import { diffLines, inlineChange, toYaml, unifiedRows } from "./yaml-diff";

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
