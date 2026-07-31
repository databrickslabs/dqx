import type { RegistryRuleOut } from "@/lib/api";

/**
 * Map a rule's polarity to the i18n key for its read-only "THEN THE RULE
 * PASSES/FAILS" line. Returns null when polarity is absent (dqx_native rules
 * carry no polarity, so no line is shown).
 */
export function polarityLineKey(
  polarity: RegistryRuleOut["polarity"] | null | undefined,
): "monitoredTables.ruleLogicThenPasses" | "monitoredTables.ruleLogicThenFails" | null {
  if (polarity === "pass") return "monitoredTables.ruleLogicThenPasses";
  if (polarity === "fail") return "monitoredTables.ruleLogicThenFails";
  return null;
}
