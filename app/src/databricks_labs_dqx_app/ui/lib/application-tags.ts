/**
 * Custom tags (business terms, ownership, pass rates, contract provenance, ...)
 * describe how a rule is used on one table, so they live on the applied rule
 * (`dq_applied_rules.user_metadata`), not on the reusable registry rule. The
 * backend drops non-reserved keys from registry rules; these helpers keep the
 * UI's split identical to `registry_models.generic_rule_metadata` /
 * `application_tags`.
 */

/** Mirrors backend `RESERVED_RULE_METADATA_KEYS`. */
export const RESERVED_RULE_METADATA_KEYS: ReadonlySet<string> = new Set([
  "name",
  "description",
  "dimension",
  "severity",
  "slot_tags",
  "pass_threshold",
]);

/** System keys stored in an applied rule's `user_metadata` that are not user tags. */
const APPLIED_SYSTEM_KEYS: ReadonlySet<string> = new Set([
  "origin",
  "column_pass_thresholds",
  "parameter_values",
  "notes",
]);

/** Keys a registry (generic) rule keeps. */
export function genericRuleMetadata(md: Record<string, unknown> | null | undefined): Record<string, unknown> {
  return Object.fromEntries(Object.entries(md ?? {}).filter(([k]) => RESERVED_RULE_METADATA_KEYS.has(k)));
}

/** String-valued custom tags from imported rule metadata, destined for the applied rule. */
export function applicationTagsFrom(md: Record<string, unknown> | null | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(md ?? {})) {
    if (RESERVED_RULE_METADATA_KEYS.has(k) || APPLIED_SYSTEM_KEYS.has(k)) continue;
    if (typeof v === "string" && v.trim()) out[k] = v.trim();
    else if (typeof v === "number" || typeof v === "boolean") out[k] = String(v);
  }
  return out;
}

/** The user-visible tags on an applied rule (excludes system keys like `origin`). */
export function appliedRuleTags(md: Record<string, unknown> | null | undefined): Record<string, string> {
  return applicationTagsFrom(md);
}

/** Re-attach the applied rule's system keys when replacing its user tags. */
export function withAppliedSystemKeys(
  existing: Record<string, unknown> | null | undefined,
  tags: Record<string, string>,
): Record<string, unknown> {
  const kept = Object.fromEntries(Object.entries(existing ?? {}).filter(([k]) => APPLIED_SYSTEM_KEYS.has(k)));
  return { ...kept, ...tags };
}

/** Stable `key=value; ...` signature for comparing tag sets across tables. */
export function tagSignature(tags: Record<string, string>): string {
  return Object.keys(tags)
    .sort()
    .map((k) => `${k}=${tags[k]}`)
    .join("; ");
}
