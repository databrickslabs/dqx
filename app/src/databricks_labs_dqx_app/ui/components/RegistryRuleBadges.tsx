import { useTranslation } from "react-i18next";
import {
  Archive,
  Clock,
  FileEdit,
  PencilLine,
  ShieldCheck,
  Sparkles,
  XCircle,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { AI_BUTTON_BG } from "@/lib/ai-style";
import type { RegistryRuleOut, RegistryRuleOutAuthorKind } from "@/lib/api";

// Reserved `user_metadata` keys with dedicated form fields — never rendered
// as free-form tags. Shared by the Rules Registry list and detail pages.
export const RESERVED_NAME_KEY = "name";
export const RESERVED_DESCRIPTION_KEY = "description";
export const RESERVED_DIMENSION_KEY = "dimension";
export const RESERVED_SEVERITY_KEY = "severity";

/**
 * Severity label-definition values are stored/seeded lowest-first
 * (``["Low", "Medium", "High", "Critical"]``) so they read naturally as an
 * ascending scale in the admin editor. Every *display* surface (filters,
 * dropdowns) wants the opposite — most severe first — so callers reverse
 * through this helper rather than re-deriving the convention inline.
 */
export function orderSeverityValuesForDisplay(values: string[]): string[] {
  return [...values].reverse();
}

export function getTag(rule: RegistryRuleOut, key: string): string {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const v = md[key];
  return typeof v === "string" ? v : "";
}

export function freeTags(rule: RegistryRuleOut): Record<string, string> {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(md)) {
    if (
      k === RESERVED_NAME_KEY ||
      k === RESERVED_DESCRIPTION_KEY ||
      k === RESERVED_DIMENSION_KEY ||
      k === RESERVED_SEVERITY_KEY
    ) {
      continue;
    }
    if (typeof v === "string") out[k] = v;
  }
  return out;
}

export interface LabelColorDefinition {
  key: string;
  value_colors?: Record<string, string> | null;
}

export function colorFor(defs: LabelColorDefinition[], key: string, value: string): string | undefined {
  const def = defs.find((d) => d.key === key);
  return def?.value_colors?.[value] ?? undefined;
}

export function ColorDot({ color }: { color?: string }) {
  if (!color) return null;
  return (
    <span
      className="inline-block h-1.5 w-1.5 rounded-full"
      style={{ backgroundColor: color }}
      aria-hidden
    />
  );
}

export function TagBadge({ label, color }: { label: string; color?: string }) {
  if (!label) return <span className="text-muted-foreground/50">—</span>;
  return (
    <Badge variant="outline" className="gap-1 text-[10px] font-normal">
      <ColorDot color={color} />
      {label}
    </Badge>
  );
}

/**
 * Severity-specific variant of `TagBadge`. Severity is the one tag with a
 * meaningful "genuinely unset" state a steward needs to notice (unlike
 * dimension, an unset severity means checks fire with no configured
 * priority) — so instead of `TagBadge`'s silent "—" it renders an explicit,
 * localized "None" badge. Used wherever a rule's severity is listed
 * read-only (registry table/picker columns, AI-suggestion preview); the
 * editable severity-override dropdown on an applied-rule card has its own
 * equivalent handling since it's not a plain badge.
 */
export function SeverityBadge({ severity, color }: { severity: string; color?: string }) {
  const { t } = useTranslation();
  if (!severity) {
    return (
      <Badge variant="outline" className="gap-1 text-[10px] font-normal text-muted-foreground">
        {t("monitoredTables.severityNoneLabel")}
      </Badge>
    );
  }
  return <TagBadge label={severity} color={color} />;
}

export function StatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "draft":
      return (
        <Badge variant="secondary" className="gap-1 text-[10px]">
          <FileEdit className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusDraft")}
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-amber-500 text-amber-600">
          <Clock className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusPendingApproval")}
        </Badge>
      );
    case "approved":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-emerald-500 text-emerald-600">
          <ShieldCheck className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusApproved")}
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-red-500 text-red-600">
          <XCircle className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusRejected")}
        </Badge>
      );
    case "deprecated":
      return (
        <Badge variant="outline" className="gap-1 text-[10px] border-muted-foreground text-muted-foreground">
          <Archive className="h-2.5 w-2.5" />
          {t("rulesRegistry.statusDeprecated")}
        </Badge>
      );
    default:
      return <Badge variant="secondary" className="text-[10px]">{status}</Badge>;
  }
}

/**
 * "Modified since vN" indicator, shown alongside the `approved` StatusBadge
 * when a published rule carries unpublished edit-in-place changes
 * (`RegistryRuleOut.modified_since_publish`). The published vN keeps serving
 * everywhere until the revision is re-submitted and re-approved as vN+1 — this
 * badge is the steward's signal that a Submit-for-review is pending.
 */
export function ModifiedBadge({ version }: { version: number }) {
  const { t } = useTranslation();
  return (
    <Badge variant="outline" className="gap-1 text-[10px] border-amber-500 text-amber-600">
      <PencilLine className="h-2.5 w-2.5" />
      {t("rulesRegistry.badgeModifiedSince", { version })}
    </Badge>
  );
}

export function ModeBadge({ mode }: { mode: string }) {
  const { t } = useTranslation();
  const label =
    mode === "dqx_native"
      ? t("rulesRegistry.modeDqxNative")
      : mode === "lowcode"
        ? t("rulesRegistry.modeLowcode")
        : t("rulesRegistry.modeSql");
  return (
    <Badge variant="secondary" className="text-[10px] font-mono">
      {label}
    </Badge>
  );
}

/**
 * Surfaces a rule's authorship (human / AI-generated / AI-assisted), read
 * from `author_kind`. AI-authored rules use the purple AI gradient styling
 * from Phase 7A; human-authored rules get a plain badge. Renders nothing
 * when the field isn't populated yet (older rows created before this field
 * existed) — see CHECKMARK item 7.
 */
export function AuthorKindBadge({ authorKind }: { authorKind?: RegistryRuleOutAuthorKind }) {
  const { t } = useTranslation();
  if (!authorKind) return null;
  if (authorKind === "human") {
    return (
      <Badge variant="outline" className="text-[10px] font-normal">
        {t("rulesRegistry.authorKindHuman")}
      </Badge>
    );
  }
  const label =
    authorKind === "ai_generated"
      ? t("rulesRegistry.authorKindAiGenerated")
      : t("rulesRegistry.authorKindAiAssisted");
  return (
    <Badge className={cn("gap-1 border-0 text-[10px] font-normal text-white", AI_BUTTON_BG)}>
      <Sparkles className="h-2.5 w-2.5" />
      {label}
    </Badge>
  );
}
