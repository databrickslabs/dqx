import { describe, expect, test } from "bun:test";
import {
  canToggleColumn,
  columnsSignature,
  indexColumns,
  moveColumn,
  toggleColumn,
  visibleColumns,
} from "./result-columns";

const columns = indexColumns([
  { name: "id", mapped: true },
  { name: "amount", mapped: false },
  { name: "id", mapped: false },
]);
const order = columns.map((c) => c.id);

describe("visibleColumns", () => {
  test("every column shows, in result order, until one is hidden", () => {
    expect(visibleColumns(columns, order, new Set()).map((c) => c.index)).toEqual([0, 1, 2]);
  });

  test("hiding one of two same-named columns leaves the other", () => {
    const shown = visibleColumns(columns, order, new Set(["0"]));
    expect(shown.map((c) => [c.index, c.name])).toEqual([
      [1, "amount"],
      [2, "id"],
    ]);
  });

  test("a reordered column keeps its own row index", () => {
    const moved = moveColumn(order, "2", "0");
    expect(visibleColumns(columns, moved, new Set()).map((c) => c.index)).toEqual([2, 0, 1]);
  });

  test("ids that are not in the result are skipped", () => {
    expect(visibleColumns(columns, ["7", "1"], new Set()).map((c) => c.index)).toEqual([1]);
  });
});

describe("toggleColumn", () => {
  test("hides a visible column and shows a hidden one", () => {
    const hidden = toggleColumn("1", order, new Set());
    expect([...hidden]).toEqual(["1"]);
    expect([...toggleColumn("1", order, hidden)]).toEqual([]);
  });

  test("the last visible column cannot be hidden", () => {
    const hidden = new Set(["0", "1"]);
    expect(canToggleColumn("2", order, hidden)).toBe(false);
    expect([...toggleColumn("2", order, hidden)].sort()).toEqual(["0", "1"]);
  });

  test("a hidden column can always be shown again", () => {
    expect(canToggleColumn("0", order, new Set(["0", "1"]))).toBe(true);
  });

  test("with two columns visible either can be hidden", () => {
    expect(canToggleColumn("1", order, new Set(["0"]))).toBe(true);
  });
});

describe("moveColumn", () => {
  test("moves a column to the position of the one it is dropped on", () => {
    expect(moveColumn(["0", "1", "2"], "0", "2")).toEqual(["1", "2", "0"]);
    expect(moveColumn(["0", "1", "2"], "2", "1")).toEqual(["0", "2", "1"]);
  });

  test("an unknown id or a drop on itself changes nothing", () => {
    expect(moveColumn(["0", "1"], "9", "1")).toEqual(["0", "1"]);
    expect(moveColumn(["0", "1"], "1", "1")).toEqual(["0", "1"]);
  });
});

describe("columnsSignature", () => {
  test("is the same for the same column names and differs when they change", () => {
    const a = [
      { name: "id", mapped: true },
      { name: "amount", mapped: false },
    ];
    expect(columnsSignature(a)).toBe(columnsSignature(a.map((c) => ({ ...c, mapped: !c.mapped }))));
    expect(columnsSignature(a)).not.toBe(columnsSignature([...a].reverse()));
    expect(columnsSignature(a)).not.toBe(columnsSignature([{ name: "id,amount", mapped: false }]));
  });
});
