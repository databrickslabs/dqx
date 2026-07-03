// MappingChips — color-indexed slot->column mapping display for an applied
// registry rule, ported 1:1 (layout/typography) from dqlake's
// `bindings/MappingChips.tsx` read-only rendering path. Each mapping GROUP
// (one entry in `column_mapping`) gets its own color "thread" so a rule
// applied to several column sets is easy to scan.
//
// DQX's backend has no "edit an existing application's mapping" endpoint —
// changing a mapping means removing and re-adding the rule via
// AddRulesDialog — so, unlike dqlake's editable chips (which open a column
// picker popover per chip), these chips are always read-only. Jump-to-column
// navigation stays available.

import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import type { AppliedRuleOutColumnMappingItem, RuleSlot } from "@/lib/api";

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

/** Count mapping entries that have at least one slot filled. */
function countNonEmpty(mapping: AppliedRuleOutColumnMappingItem[], slotNames: string[]): number {
  return mapping.filter((entry) => slotNames.some((s) => Boolean(entry[s]))).length;
}

function FamilyBadge({ family }: { family: string }) {
  if (!family) return null;
  return (
    <span className="inline-block rounded bg-muted/60 border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground font-medium uppercase tracking-wide shrink-0">
      {family}
    </span>
  );
}

function ReadonlyChip({
  colorClass,
  label,
  onJump,
  onRemove,
  removeTitle,
}: {
  colorClass: string;
  label: string;
  onJump?: () => void;
  /** When set, renders an "x" affordance that removes the whole mapping
   *  group this chip belongs to (every chip for the same group, across
   *  every slot row, shares the same group index). */
  onRemove?: () => void;
  removeTitle?: string;
}) {
  return (
    <span className={cn("inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs font-mono", colorClass)}>
      {onJump ? (
        <button
          type="button"
          onClick={onJump}
          className="cursor-pointer hover:underline focus:outline-none"
        >
          {label}
        </button>
      ) : (
        label
      )}
      {onRemove && (
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onRemove();
          }}
          title={removeTitle}
          aria-label={removeTitle}
          className="ml-0.5 opacity-60 hover:opacity-100 focus:outline-none leading-none"
        >
          ×
        </button>
      )}
    </span>
  );
}

interface MappingChipsProps {
  /** One mapping GROUP per materialized check: slot-name -> column-name. */
  columnMapping: AppliedRuleOutColumnMappingItem[];
  /** Declared slots for the rule. When provided, chips render per-slot rows
   *  (matching dqlake's layout: `{{slot}}` + family badge -> chips). When
   *  omitted, falls back to one flat chip row per mapping group. */
  slots?: RuleSlot[];
  /** Jump to a column's card in the by-column lens. */
  onJumpToColumn?: (colName: string) => void;
  /** Removes the mapping group at this index (every chip for that group,
   *  across every slot row). Omit to render fully read-only chips with no
   *  remove affordance. */
  onRemoveGroup?: (groupIdx: number) => void;
  /** Opens the "apply this rule to another column" flow, which stages a new
   *  mapping group (and therefore a new applied-check entry) for this rule.
   *  Rendered as a dashed "+ Apply to another column" button below the last
   *  slot row, matching dqlake's affordance. Omit to hide it. */
  onAddGroup?: () => void;
  className?: string;
}

export function MappingChips({
  columnMapping,
  slots,
  onJumpToColumn,
  onRemoveGroup,
  onAddGroup,
  className,
}: MappingChipsProps) {
  const { t } = useTranslation();
  if (columnMapping.length === 0 && !onAddGroup) return null;

  // No declared slots (e.g. aggregate rule) — fall back to the flat
  // per-group rendering used before slots were threaded through.
  if (!slots || slots.length === 0) {
    return (
      <div className={cn("space-y-1", className)}>
        {columnMapping.map((group, groupIdx) => (
          <div key={groupIdx} className="flex flex-wrap gap-1">
            {Object.entries(group).map(([slot, column]) => (
              <ReadonlyChip
                key={slot}
                colorClass={paletteAt(groupIdx)}
                label={`${slot} → ${column}`}
                onJump={onJumpToColumn ? () => onJumpToColumn(column) : undefined}
              />
            ))}
          </div>
        ))}
      </div>
    );
  }

  const slotNames = slots.map((s) => s.name);
  const n = countNonEmpty(columnMapping, slotNames);

  return (
    <div className={cn("space-y-2", className)}>
      <div className="flex items-center gap-2">
        <span className="text-[10px] font-semibold uppercase tracking-widest text-blue-500 dark:text-blue-400">
          {t("monitoredTables.mappingCheckCount", { count: n })}
        </span>
      </div>

      <div className="space-y-2">
        {slots.map((slot, slotIdx) => {
          const isLastSlot = slotIdx === slots.length - 1;
          const filled = columnMapping
            .map((group, groupIdx) => ({ colName: group[slot.name], groupIdx }))
            .filter((e): e is { colName: string; groupIdx: number } => Boolean(e.colName));

          return (
            <div key={slot.name} className="grid grid-cols-[160px_24px_1fr] items-center gap-3">
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-mono text-xs truncate">{`{{${slot.name}}}`}</span>
                <FamilyBadge family={slot.family} />
              </div>
              <span className="text-muted-foreground text-xs justify-self-center self-center">&rarr;</span>
              <div className="flex flex-wrap items-center gap-1.5">
                {filled.length === 0 && !onAddGroup ? (
                  <span className="text-xs text-muted-foreground italic">
                    {t("monitoredTables.noColumnMapped")}
                  </span>
                ) : (
                  filled.map(({ colName, groupIdx }) => (
                    <ReadonlyChip
                      key={groupIdx}
                      colorClass={paletteAt(groupIdx)}
                      label={colName}
                      onJump={onJumpToColumn ? () => onJumpToColumn(colName) : undefined}
                      onRemove={onRemoveGroup ? () => onRemoveGroup(groupIdx) : undefined}
                      removeTitle={t("monitoredTables.removeMappingGroupTitle", { count: groupIdx + 1 })}
                    />
                  ))
                )}

                {/* + Apply to another column: only on the last slot row */}
                {onAddGroup && isLastSlot && (
                  <button
                    type="button"
                    onClick={onAddGroup}
                    className="text-xs text-muted-foreground hover:text-foreground border border-dashed border-border rounded px-2 py-0.5"
                  >
                    {t("monitoredTables.applyToAnotherColumnButton")}
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
