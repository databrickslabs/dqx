import { useTranslation } from "react-i18next";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

/**
 * `include_drafts` query-param value for the current run-mode selection:
 * `true` ONLY when the dropdown says "Published + Draft", else `undefined`
 * so the param is omitted entirely and the backend's published-only
 * default applies (the params object never carries an explicit `false`).
 * Exported for the run-mode wiring tests.
 */
export function includeDraftsParam(includeDrafts: boolean): true | undefined {
  return includeDrafts ? true : undefined;
}

/**
 * Run-mode selector for the results surfaces: "Published only" (default —
 * the dq-results endpoints exclude draft runs) vs "Published + Draft"
 * (passes `include_drafts=true` to every dq-results query on the surface).
 * State is owned PER SURFACE by the caller — the selection is deliberately
 * not global, so switching it on one tab never changes another.
 */
export function RunModeSelect({
  includeDrafts,
  onChange,
}: {
  includeDrafts: boolean;
  onChange: (includeDrafts: boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <Select
      value={includeDrafts ? "published-draft" : "published"}
      onValueChange={(v) => onChange(v === "published-draft")}
    >
      <SelectTrigger size="sm" className="text-xs" aria-label={t("resultsUi.runModeAria")}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent align="end">
        <SelectItem value="published">{t("resultsUi.runModePublishedOnly")}</SelectItem>
        <SelectItem value="published-draft">{t("resultsUi.runModePublishedDraft")}</SelectItem>
      </SelectContent>
    </Select>
  );
}
