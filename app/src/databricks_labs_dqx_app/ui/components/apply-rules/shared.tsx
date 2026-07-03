// Shared helpers for the Apply Rules tab components (registry-rule tag
// lookups, label coloring, API error extraction). Kept in one place so
// AddRulesDialog / RuleConfigCard / RulesByColumn agree on the same
// conventions instead of re-deriving them.

import { Badge } from "@/components/ui/badge";
import type { RegistryRuleOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";

export const RESERVED_NAME_KEY = "name";
export const RESERVED_DIMENSION_KEY = "dimension";
export const RESERVED_SEVERITY_KEY = "severity";

export function getTag(rule: RegistryRuleOut, key: string): string {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const v = md[key];
  return typeof v === "string" ? v : "";
}

export function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

export function colorFor(defs: LabelDefinition[], key: string, value: string): string | undefined {
  const def = defs.find((d) => d.key === key);
  return def?.value_colors?.[value] ?? undefined;
}

export function TagBadge({ label, color }: { label: string; color?: string }) {
  if (!label) return null;
  return (
    <Badge variant="outline" className="gap-1 text-[10px] font-normal">
      {color && (
        <span className="inline-block h-1.5 w-1.5 rounded-full" style={{ backgroundColor: color }} aria-hidden />
      )}
      {label}
    </Badge>
  );
}
