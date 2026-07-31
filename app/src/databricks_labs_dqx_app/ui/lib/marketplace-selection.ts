import type { CheckFunctionDef, MarketplacePackOut, MarketplaceRuleOut, RegistryRuleOut, RuleDefinition } from "@/lib/api";
import { parseDqxCheckJson } from "@/lib/registry-rule-conversion";

// ---------------------------------------------------------------------------
// Tag display + ordering
// ---------------------------------------------------------------------------

/** Short tags that read as acronyms rather than words — upper-cased whole. */
const TAG_ACRONYMS = new Set(["us", "uk", "eu", "usa", "crm", "fsi", "emea", "apac", "latam"]);

/**
 * Human display label for an industry/region tag value. Acronyms upper-case
 * whole (``us`` → ``US``); everything else is sentence-cased (``banking`` →
 * ``Banking``, ``new zealand`` → ``New zealand``). ``all`` is handled by the
 * caller (localized), so it is passed through unchanged here.
 */
export function formatTagLabel(value: string): string {
  if (value === "all") return value;
  const lower = value.toLowerCase();
  if (TAG_ACRONYMS.has(lower)) return lower.toUpperCase();
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

/**
 * Region tier for the tiered ordering the UI wants: broad → narrow
 * (``global`` → macro-region → country). Lower tier sorts first; within a
 * tier, values sort A–Z. Example order: ``global, eu, australia, canada, uk,
 * us``. Unknown values default to the country tier so a new country tag slots
 * in sensibly without code changes.
 */
const REGION_TIER: Record<string, number> = { global: 0, emea: 1, apac: 1, latam: 1, eu: 1 };
const REGION_COUNTRY_TIER = 2;

/** Region tier (0 = global, 1 = macro-region, 2 = country) for a tag value.
 * Exported so the filter bar can draw a divider between tier groups. The
 * synthetic "all" chip is tier -1 so it always leads. */
export function regionTier(value: string): number {
  if (value === "all") return -1;
  return REGION_TIER[value.toLowerCase()] ?? REGION_COUNTRY_TIER;
}

/** Compare two region tags by (tier asc, then A–Z). */
export function compareRegions(a: string, b: string): number {
  const ta = regionTier(a);
  const tb = regionTier(b);
  if (ta !== tb) return ta - tb;
  return a.localeCompare(b);
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

function collectTag(
  packs: MarketplacePackOut[],
  pick: (r: MarketplaceRuleOut) => string[],
  sort: (a: string, b: string) => number,
): string[] {
  const set = new Set<string>();
  for (const p of packs) for (const r of p.rules) for (const tag of pick(r)) set.add(tag);
  return ["all", ...[...set].sort(sort)];
}

export function collectIndustries(packs: MarketplacePackOut[]): string[] {
  return collectTag(packs, (r) => r.industries, (a, b) => a.localeCompare(b));
}

export function collectRegions(packs: MarketplacePackOut[]): string[] {
  return collectTag(packs, (r) => r.regions, compareRegions);
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

// ---------------------------------------------------------------------------
// Marketplace preview helper
// ---------------------------------------------------------------------------

const EMPTY_DEFINITION: RuleDefinition = { body: {}, slots: [], parameters: [] };

/**
 * Build a minimal {@link RegistryRuleOut}-shaped object from a marketplace
 * rule's normalized check dict so the existing {@link RuleLogicDisclosure}
 * component can render it without modification.
 *
 * Returns `undefined` if the check function cannot be resolved. Callers MUST
 * pass a fully-loaded *checkFunctions* list — an empty or not-yet-loaded list
 * causes every lookup to fail and always yields `undefined` → blank preview.
 */
export function checkDictToPreviewRule(
  rule: MarketplaceRuleOut,
  checkFunctions: CheckFunctionDef[],
  t: (key: string, opts?: Record<string, unknown>) => string,
): RegistryRuleOut | undefined {
  try {
    const parsed = parseDqxCheckJson(
      JSON.stringify(rule.check),
      EMPTY_DEFINITION,
      {},
      checkFunctions,
      t,
    );
    return {
      rule_id: rule.rule_key,
      mode: parsed.mode,
      status: "draft",
      version: 1,
      polarity: parsed.polarity ?? null,
      author_kind: "human",
      definition: parsed.definition,
      user_metadata: parsed.userMetadata,
      is_builtin: false,
      modified_since_publish: false,
      display_status: "draft",
    } as RegistryRuleOut;
  } catch {
    return undefined;
  }
}
