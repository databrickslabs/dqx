import type { MarketplacePackOut, MarketplaceRuleOut, RegistryRuleOut } from "@/lib/api";

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

// ---------------------------------------------------------------------------
// Marketplace filtering & selection helpers
// ---------------------------------------------------------------------------

export interface MarketplaceFilters {
  industry: string; // "all" or a taxonomy value
  region: string; // "all" or a taxonomy value
  search: string;
}

export function ruleMatchesFilters(rule: MarketplaceRuleOut, f: MarketplaceFilters): boolean {
  const industryOk =
    f.industry === "all" || rule.industries.length === 0 || rule.industries.includes(f.industry);
  const regionOk = f.region === "all" || rule.regions.length === 0 || rule.regions.includes(f.region);
  const q = f.search.trim().toLowerCase();
  const searchOk =
    q === "" || rule.name.toLowerCase().includes(q) || rule.description.toLowerCase().includes(q);
  return industryOk && regionOk && searchOk;
}

function collectTag(packs: MarketplacePackOut[], pick: (r: MarketplaceRuleOut) => string[]): string[] {
  const set = new Set<string>();
  for (const p of packs) for (const r of p.rules) for (const tag of pick(r)) set.add(tag);
  return ["all", ...[...set].sort()];
}

export function collectIndustries(packs: MarketplacePackOut[]): string[] {
  return collectTag(packs, (r) => r.industries);
}

export function collectRegions(packs: MarketplacePackOut[]): string[] {
  return collectTag(packs, (r) => r.regions);
}

export function packSelectionState(
  packRuleKeys: string[],
  selected: Set<string>,
): "none" | "some" | "all" {
  if (packRuleKeys.length === 0) return "none";
  const n = packRuleKeys.filter((k) => selected.has(k)).length;
  if (n === 0) return "none";
  if (n === packRuleKeys.length) return "all";
  return "some";
}

export function toggleRule(selected: Set<string>, key: string): Set<string> {
  const next = new Set(selected);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

export function togglePack(selected: Set<string>, packRuleKeys: string[]): Set<string> {
  const next = new Set(selected);
  const allSelected = packRuleKeys.every((k) => next.has(k));
  if (allSelected) for (const k of packRuleKeys) next.delete(k);
  else for (const k of packRuleKeys) next.add(k);
  return next;
}

export function selectedCheckDicts(
  packs: MarketplacePackOut[],
  selected: Set<string>,
): Record<string, unknown>[] {
  const dicts: Record<string, unknown>[] = [];
  for (const p of packs)
    for (const r of p.rules) if (selected.has(r.rule_key)) dicts.push(r.check as Record<string, unknown>);
  return dicts;
}
