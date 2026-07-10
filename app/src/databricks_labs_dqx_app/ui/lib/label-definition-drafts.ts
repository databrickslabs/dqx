/**
 * Pure draft <-> API mapping helpers for the admin Label Definitions editor
 * on the config page (`routes/_sidebar/config.tsx`).
 *
 * A `DraftDefinition` is the editor's local working copy of a
 * `LabelDefinition` — same shape plus a client-only `draftId` used as the
 * stable React key while rows are added/removed. `draftToDef` is where all
 * save-time normalisation lives: values are trimmed, and the per-value maps
 * (`value_colors` / `value_descriptions` / `value_criticality`) are pruned to
 * surviving values and validated, mirroring the server-side pruning in
 * `backend/routes/v1/config.py::save_label_definitions` so the optimistic
 * dirty-diff (`JSON.stringify` against the server copy) stays stable.
 *
 * Extracted from `config.tsx` so this logic is unit-testable without
 * rendering the route — see `label-definition-drafts.test.ts`.
 */
import type { LabelDefinition } from "@/lib/api-custom";

export const HEX_COLOR_RE = /^#[0-9A-Fa-f]{6}$/;

/** The two DQX criticalities a severity value can map to — mirrors the
 * backend's `LabelDefinitionModel` validator (`"warn" | "error"` only). */
export const CRITICALITY_VALUES = ["warn", "error"] as const;
export type CriticalityValue = (typeof CRITICALITY_VALUES)[number];

export interface DraftDefinition extends LabelDefinition {
  draftId: string;
}

export function defToDraft(d: LabelDefinition): DraftDefinition {
  return {
    draftId: crypto.randomUUID(),
    key: d.key,
    description: d.description ?? "",
    values: [...d.values],
    allow_custom_values: !!d.allow_custom_values,
    value_colors: d.value_colors ? { ...d.value_colors } : null,
    value_descriptions: d.value_descriptions ? { ...d.value_descriptions } : null,
    value_criticality: d.value_criticality ? { ...d.value_criticality } : null,
    is_builtin: !!d.is_builtin,
  };
}

export function draftToDef(d: DraftDefinition): LabelDefinition {
  const values = d.values.map((v) => v.trim()).filter(Boolean);
  const valueSet = new Set(values);
  const colors: Record<string, string> = {};
  for (const [value, color] of Object.entries(d.value_colors ?? {})) {
    if (valueSet.has(value) && HEX_COLOR_RE.test(color)) colors[value] = color;
  }
  const descriptions: Record<string, string> = {};
  for (const [value, desc] of Object.entries(d.value_descriptions ?? {})) {
    if (valueSet.has(value) && desc.trim()) descriptions[value] = desc;
  }
  const criticality: Record<string, string> = {};
  for (const [value, crit] of Object.entries(d.value_criticality ?? {})) {
    if (valueSet.has(value) && (CRITICALITY_VALUES as readonly string[]).includes(crit)) {
      criticality[value] = crit;
    }
  }
  return {
    key: d.key.trim(),
    description: (d.description ?? "").trim(),
    values,
    allow_custom_values: d.allow_custom_values,
    value_colors: Object.keys(colors).length > 0 ? colors : null,
    value_descriptions: Object.keys(descriptions).length > 0 ? descriptions : null,
    value_criticality: Object.keys(criticality).length > 0 ? criticality : null,
    is_builtin: !!d.is_builtin,
  };
}
