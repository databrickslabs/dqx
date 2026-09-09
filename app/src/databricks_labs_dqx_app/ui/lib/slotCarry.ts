import type { RuleSlot } from "@/lib/api";

/**
 * Merge carried slots (from the previous authoring mode) into a function's
 * canonical signature slots when switching INTO dqx_native.
 *
 * Strategy: overlay the carried slot's name + family onto the signature's
 * positional slots — preserving whatever the author had named/typed, rather
 * than discarding it and re-seeding. For each matched position, `arg_key` and
 * `position` always come from the signature (they reflect the function's
 * semantics, not the carried state), but `name` and `family` come from the
 * carried slot where available.
 *
 * When *carried* is empty (brand-new rule, never had any slots) the signature
 * slots are returned unchanged — this preserves the existing seed behaviour.
 *
 * Extra carried slots beyond the check's arity are intentionally dropped when
 * switching to a built-in check — the native check should show only the columns
 * its signature defines.
 */
export function mergeCarriedSlotsIntoSignature(signature: RuleSlot[], carried: RuleSlot[]): RuleSlot[] {
  if (carried.length === 0) return signature;
  const merged: RuleSlot[] = signature.map((sigSlot, i) => {
    const c = carried[i];
    if (!c) return sigSlot;
    return {
      ...sigSlot,
      name: c.name || sigSlot.name,
      // Only carry the family when it's compatible (non-"any") and the
      // signature doesn't lock a specific family. If the signature already
      // specifies a non-"any" family, keep it — the check's semantics win.
      family: sigSlot.family !== "any" ? sigSlot.family : c.family !== "any" ? c.family : sigSlot.family,
    };
  });
  return merged;
}
