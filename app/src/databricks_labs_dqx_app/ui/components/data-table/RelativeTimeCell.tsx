import { useTranslation } from "react-i18next";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { formatDateTime, getRelativeTimeParts } from "@/lib/format-utils";

/**
 * Shared "time-since" cell for overview tables (Rules Registry, Monitored
 * Tables, Data Products) — renders dqlake-style relative text ("just now" /
 * "5m ago" / "3h ago" / "2d ago") with the absolute timestamp available via
 * an in-app tooltip (not the native `title` attribute) on hover.
 *
 * Single source of truth for this pattern — previously duplicated per table.
 */
export function RelativeTimeCell({ iso }: { iso: string | null | undefined }) {
  const { t } = useTranslation();
  const rel = getRelativeTimeParts(iso);
  if (!rel) return <span className="text-muted-foreground">—</span>;
  const label =
    rel.key === "justNow"
      ? t("common.relativeJustNow")
      : t(`common.relative${rel.key[0].toUpperCase()}${rel.key.slice(1)}`, { count: rel.count });
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="cursor-default">{label}</span>
      </TooltipTrigger>
      <TooltipContent side="top">{formatDateTime(iso)}</TooltipContent>
    </Tooltip>
  );
}
