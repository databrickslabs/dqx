import { describe, expect, it } from "bun:test";
import { ACTIONS_COLUMN_KEY, splitActionsColumn } from "./sticky-actions";

type Key = "name" | "owner" | typeof ACTIONS_COLUMN_KEY;

describe("splitActionsColumn", () => {
  it("drops Actions from the order and visible keys and reports it visible", () => {
    const order: Key[] = ["name", ACTIONS_COLUMN_KEY, "owner"];
    expect(splitActionsColumn(order, order)).toEqual({
      order: ["name", "owner"],
      visibleKeys: ["name", "owner"],
      actionsVisible: true,
    });
  });

  it("reports Actions hidden when it is toggled off", () => {
    const order: Key[] = ["name", "owner", ACTIONS_COLUMN_KEY];
    expect(splitActionsColumn(order, ["name"])).toEqual({
      order: ["name", "owner"],
      visibleKeys: ["name"],
      actionsVisible: false,
    });
  });
});
