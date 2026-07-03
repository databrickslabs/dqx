import { ChevronRight } from "lucide-react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import {
  NEUTRAL_BAR_CLASS,
  completeBarClass,
  compactNumber,
  typeGroup,
  typePillClasses,
} from "@/lib/profile-format";
import { ProfileColumnExpanded } from "./ProfileColumnExpanded";

interface Props {
  name: string;
  sparkType: string | undefined;
  stats: Record<string, unknown>;
  rowCount: number | null | undefined;
  open: boolean;
  onToggle: () => void;
}

function pctOrNull(num: number | null | undefined, total: number | null | undefined): number | null {
  if (num == null || total == null || total <= 0) return null;
  return (num / total) * 100;
}

function fmtPctLabel(p: number): string {
  if (p >= 99.95) return "100%";
  if (p >= 10) return `${p.toFixed(0)}%`;
  return `${p.toFixed(1)}%`;
}

export function ProfileColumnCard({ name, sparkType, stats, rowCount, open, onToggle }: Props) {
  const { t } = useTranslation();
  const group = typeGroup(sparkType);
  const distinct = (stats.count_distinct as number | undefined) ?? null;
  const nonNull = (stats.count_non_null as number | undefined) ?? null;
  // Total denominator: prefer DQX's own `count` (rows the column observed),
  // fall back to the profile-level row count.
  const total = (stats.count as number | undefined) ?? rowCount ?? null;
  const completePct = pctOrNull(nonNull, total);
  const distinctPct = pctOrNull(distinct, total);

  return (
    <div
      id={`profile-column-card-${name}`}
      data-profile-card
      className={cn(
        "rounded-md border bg-card transition-[border-color,box-shadow] duration-150",
        open && "border-foreground/40 shadow-sm",
      )}
    >
      <button
        type="button"
        onClick={onToggle}
        className="w-full flex items-center gap-3 px-3 py-2.5 text-left hover:bg-muted/30"
      >
        <ChevronRight
          className={cn(
            "h-3.5 w-3.5 text-muted-foreground transition-transform duration-200 shrink-0",
            open && "rotate-90",
          )}
        />
        {/* Name + type pill — fills the row; stats pinned to the right */}
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <span className="font-mono font-semibold text-xs truncate">{name}</span>
          <span
            className={cn(
              "inline-flex items-center px-1.5 py-0.5 border rounded text-[10px] font-mono shrink-0",
              typePillClasses(group),
            )}
            title={sparkType ?? "unknown"}
          >
            {sparkType ?? "unknown"}
          </span>
        </div>

        {/* Stats columns — fixed width, sit close to the name */}
        <div className="flex items-center gap-2 w-[140px] shrink-0">
          <div className="h-1 w-14 bg-muted rounded overflow-hidden">
            <div
              className={cn(
                "h-full transition-[width] duration-200",
                completePct == null ? NEUTRAL_BAR_CLASS : completeBarClass(completePct),
              )}
              style={{ width: `${completePct ?? 0}%` }}
            />
          </div>
          <span className="text-[10px] text-muted-foreground font-mono whitespace-nowrap">
            {completePct == null
              ? "—"
              : t("monitoredTables.profileCompleteSuffix", { pct: fmtPctLabel(completePct) })}
          </span>
        </div>

        <div className="flex items-center gap-2 w-[140px] shrink-0">
          <div className="h-1 w-14 bg-muted rounded overflow-hidden">
            <div className={cn("h-full transition-[width] duration-200", NEUTRAL_BAR_CLASS)} style={{ width: `${distinctPct ?? 0}%` }} />
          </div>
          <span className="text-[10px] text-muted-foreground font-mono whitespace-nowrap">
            {distinctPct == null
              ? distinct == null
                ? "—"
                : t("monitoredTables.profileDistinctCountSuffix", { count: compactNumber(distinct) })
              : t("monitoredTables.profileDistinctSuffix", { pct: fmtPctLabel(distinctPct) })}
          </span>
        </div>
      </button>
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-[220ms] ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="min-h-0 overflow-hidden border-t border-border">
          <ProfileColumnExpanded stats={stats} rowCount={rowCount} sparkType={sparkType} />
        </div>
      </div>
    </div>
  );
}
