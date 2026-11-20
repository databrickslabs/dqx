/**
 * Pure helpers for admin-configured mandatory tags and default tag values on
 * the rule editor. Mirrors `apply_label_defaults` / `missing_required_labels`
 * in `backend/registry_models.py` so the form's gating matches the submit
 * route's server-side check.
 */
import type { LabelDefinition } from "@/lib/api-custom";
import { SELF_GATED_TAG_KEYS } from "@/lib/label-definition-drafts";

/** Empty catalog + custom values disallowed → a "true" presence tag. */
function isBooleanTag(def: LabelDefinition): boolean {
  return def.values.length === 0 && !def.allow_custom_values;
}

/** Whether the record itself carries the key, ignoring Object.prototype members. */
function hasOwnTag(tags: Record<string, string>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(tags, key);
}

/** The value a new rule starts with for this tag, or `null` when it starts without it. */
export function initialTagValue(def: LabelDefinition): string | null {
  if (def.default_value && def.values.includes(def.default_value)) return def.default_value;
  if (!def.required) return null;
  // A mandatory tag with no default still appears on the rule — blank, for
  // the author to fill in — except a boolean tag, whose only value is "true".
  return isBooleanTag(def) ? "true" : "";
}

/**
 * Add every defaulted / mandatory tag *tags* does not carry yet. Tags already
 * present are never touched. Returns *tags* itself when nothing was added.
 */
export function seedTagDefaults(
  tags: Record<string, string>,
  definitions: LabelDefinition[],
): Record<string, string> {
  let out = tags;
  for (const def of definitions) {
    if (!def.key || hasOwnTag(out, def.key)) continue;
    const value = initialTagValue(def);
    if (value !== null) out = { ...out, [def.key]: value };
  }
  return out;
}

/** Keys of mandatory tags that are missing from *tags* or have an empty value. */
export function missingRequiredTags(
  tags: Record<string, string>,
  definitions: LabelDefinition[],
): string[] {
  return definitions
    .filter(
      (d) =>
        d.required &&
        !SELF_GATED_TAG_KEYS.has(d.key) &&
        (!hasOwnTag(tags, d.key) || !(tags[d.key] ?? "").trim()),
    )
    .map((d) => d.key);
}
