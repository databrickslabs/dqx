import { describe, expect, test } from "bun:test";
import { mergeCarriedSlotsIntoSignature } from "./slotCarry";
import type { RuleSlot } from "@/lib/api";

const slot = (name: string, family = "any"): RuleSlot =>
  ({ name, family, position: 0 } as RuleSlot);

describe("mergeCarriedSlotsIntoSignature", () => {
  test("maps carried names onto signature positions", () => {
    const sig = [slot("column_1"), slot("column_2")];
    const carried = [slot("id"), slot("region")];
    const out = mergeCarriedSlotsIntoSignature(sig, carried);
    expect(out.map((s) => s.name)).toEqual(["id", "region"]);
  });
  test("does NOT append carried slots beyond signature arity", () => {
    const sig = [slot("column_1")]; // check takes 1 column
    const carried = [slot("id"), slot("extra_a"), slot("extra_b")];
    const out = mergeCarriedSlotsIntoSignature(sig, carried);
    expect(out).toHaveLength(1);
    expect(out[0].name).toBe("id");
  });
  test("returns signature unchanged when carried is empty", () => {
    const sig = [slot("column_1")];
    expect(mergeCarriedSlotsIntoSignature(sig, [])).toEqual(sig);
  });
});
