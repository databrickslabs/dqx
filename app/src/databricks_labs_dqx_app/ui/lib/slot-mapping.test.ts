import { describe, expect, test } from "bun:test";
import type { ColumnOut, RuleSlot } from "./api";
import { buildGenericSlotMapping } from "./slot-mapping";

const col = (name: string, type_name = "string") => ({ name, type_name }) as ColumnOut;
const slot = (name: string, position = 0) =>
  ({ name, family: "any", position, cardinality: "one" }) as RuleSlot;

describe("buildGenericSlotMapping", () => {
  const columns = [col("customer_id"), col("email")];

  test("keys the mapping by the generic slot, resolving the column from the input slot", () => {
    expect(buildGenericSlotMapping([slot("column")], { customer_id: "column" }, columns)).toEqual({
      column: "customer_id",
    });
  });

  test("without renames it behaves like same-name matching", () => {
    expect(buildGenericSlotMapping([slot("email")], {}, columns)).toEqual({ email: "email" });
  });

  test("returns null when the input column isn't on the table", () => {
    expect(buildGenericSlotMapping([slot("column")], { phone: "column" }, columns)).toBeNull();
  });

  test("maps multi-slot rules positionally", () => {
    expect(
      buildGenericSlotMapping(
        [slot("column_1", 0), slot("column_2", 1)],
        { email: "column_1", customer_id: "column_2" },
        columns,
      ),
    ).toEqual({ column_1: "email", column_2: "customer_id" });
  });
});
