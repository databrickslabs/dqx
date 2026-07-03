// MappingChips — color-indexed slot->column mapping display for an applied
// registry rule. Each mapping GROUP (one entry in `column_mapping`) gets its
// own color "thread" so a rule applied to several column sets is easy to
// scan. Presentational only — editing a mapping happens by removing and
// re-adding the rule via AddRulesDialog, so these chips are read-only.

import { cn } from "@/lib/utils";
import type { AppliedRuleOutColumnMappingItem } from "@/lib/api";

const PALETTE = [
  "bg-emerald-500/10 text-emerald-700 border-emerald-500/30 dark:text-emerald-400",
  "bg-blue-500/10 text-blue-700 border-blue-500/30 dark:text-blue-400",
  "bg-violet-500/10 text-violet-700 border-violet-500/30 dark:text-violet-400",
  "bg-orange-500/10 text-orange-700 border-orange-500/30 dark:text-orange-400",
  "bg-pink-500/10 text-pink-700 border-pink-500/30 dark:text-pink-400",
  "bg-cyan-500/10 text-cyan-700 border-cyan-500/30 dark:text-cyan-400",
] as const;

export function paletteAt(index: number): string {
  return PALETTE[index % PALETTE.length];
}

interface MappingChipsProps {
  /** One mapping GROUP per materialized check: slot-name -> column-name. */
  columnMapping: AppliedRuleOutColumnMappingItem[];
  className?: string;
}

/**
 * Renders every slot->column pair across all mapping groups as small chips,
 * colored per group index so groups are visually distinguishable when a rule
 * is applied to the table more than once (e.g. the same range check on two
 * different numeric columns).
 */
export function MappingChips({ columnMapping, className }: MappingChipsProps) {
  if (columnMapping.length === 0) return null;
  return (
    <div className={cn("space-y-1", className)}>
      {columnMapping.map((group, groupIdx) => (
        <div key={groupIdx} className="flex flex-wrap gap-1">
          {Object.entries(group).map(([slot, column]) => (
            <span
              key={slot}
              className={cn(
                "inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-mono",
                paletteAt(groupIdx),
              )}
            >
              {slot} → {column}
            </span>
          ))}
        </div>
      ))}
    </div>
  );
}
