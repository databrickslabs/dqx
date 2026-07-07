// MappingChips — color-indexed slot->column mapping display for an applied
// registry rule, ported 1:1 (layout/typography) from dqlake's
// `bindings/MappingChips.tsx`. Each mapping GROUP (one entry in
// `column_mapping`) gets its own color "thread" so a rule applied to
// several column sets is easy to scan.
//
// The Apply Rules tab is now a staged local editor (P16-F) with no
// per-mapping-change network call, so — mirroring dqlake's editable chips —
// a chip can be clicked to reassign its column via the same family-filtered
// column-picker popover (`ColumnDropdownList`) `SingleColumnPicker` uses.
// Pass `onChangeGroup` to enable this; omit it (e.g. the AI-suggestion
// preview list, which has no staged row to edit) to fall back to
// non-interactive display chips.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";
import type { AppliedRuleOutColumnMappingItem, ColumnOut, RuleSlot } from "@/lib/api";
import { ColumnDropdownList, MultiColumnPicker, columnsForSlot } from "./ColumnPicker";

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
  busy,
}: {
  colorClass: string;
  label: string;
  onJump?: () => void;
  /** When set, renders an "x" affordance that removes the whole mapping
   *  group this chip belongs to (every chip for the same group, across
   *  every slot row, shares the same group index). */
  onRemove?: () => void;
  removeTitle?: string;
  /** Removal request for this group is in flight — disables the "x" and
   *  shows a spinner instead. */
  busy?: boolean;
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
          disabled={busy}
          onClick={(e) => {
            e.stopPropagation();
            onRemove();
          }}
          title={removeTitle}
          aria-label={removeTitle}
          className="ml-0.5 opacity-60 hover:opacity-100 focus:outline-none leading-none disabled:opacity-40"
        >
          {busy ? <Loader2 className="h-2.5 w-2.5 animate-spin inline-block" aria-hidden /> : "×"}
        </button>
      )}
    </span>
  );
}

/** Clickable chip that opens a family-filtered column-picker popover to
 *  reassign this slot/group's column, mirroring dqlake's editable chip. The
 *  trigger itself still renders as a compact colored chip (not a full
 *  combobox button like `SingleColumnPicker`) so the slot-row layout stays
 *  identical to the read-only rendering. */
function EditableChip({
  colorClass,
  label,
  slot,
  columns,
  excludeColumns,
  onChange,
  onJump,
  onRemove,
  removeTitle,
}: {
  colorClass: string;
  label: string;
  slot: RuleSlot;
  columns: ColumnOut[];
  excludeColumns: string[];
  onChange: (colName: string) => void;
  onJump?: () => void;
  onRemove?: () => void;
  removeTitle?: string;
}) {
  const { t } = useTranslation();
  // Controlled open so the popover closes on selection, mirroring
  // `SingleColumnPicker`'s click-to-select-and-commit behavior — an
  // uncontrolled Popover would stay open after picking a column.
  const [open, setOpen] = useState(false);
  // The chip's own current column must stay selectable even though it's
  // technically "in use" — only exclude *other* groups' columns from the
  // candidate list.
  const matches = columnsForSlot(columns, slot, excludeColumns.filter((c) => c !== label));
  return (
    <span className={cn("inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs font-mono", colorClass)}>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <button
            type="button"
            onClick={(e) => e.stopPropagation()}
            className="cursor-pointer hover:underline focus:outline-none"
          >
            {label}
          </button>
        </PopoverTrigger>
        <PopoverContent className="w-80 p-0" align="start" onClick={(e) => e.stopPropagation()}>
          <ColumnDropdownList
            slot={slot}
            matches={matches}
            totalAll={columns.length}
            onSelect={(colName) => {
              onChange(colName);
              setOpen(false);
            }}
          />
        </PopoverContent>
      </Popover>
      {onJump && (
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onJump();
          }}
          title={t("monitoredTables.jumpToColumnTitle")}
          aria-label={t("monitoredTables.jumpToColumnTitle")}
          className="opacity-60 hover:opacity-100 focus:outline-none leading-none"
        >
          &#x2197;
        </button>
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

/**
 * One slot's placeholder chip in the "add a new mapping group" flow —
 * renders directly inline in that slot's row (not a separate form/dialog
 * below the component). Mirrors `EditableChip`'s Popover, but starts
 * auto-opened when this slot is the next one needing a value, so the
 * picker for slot N+1 pops open immediately once slot N is picked — no
 * extra click to "open" it, matching the single-slot case where pressing
 * "+ Apply to another column" opens the picker directly.
 */
function PendingSlotChip({
  slot,
  columns,
  excludeColumns,
  value,
  autoOpen,
  colorClass,
  onSelect,
  onCancel,
}: {
  slot: RuleSlot;
  columns: ColumnOut[];
  excludeColumns: string[];
  value?: string;
  autoOpen: boolean;
  colorClass: string;
  onSelect: (colName: string) => void;
  /** Rendered only on the flow's cancel affordance (last slot row). */
  onCancel?: () => void;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(autoOpen);
  // `autoOpen` flips true when this becomes the next slot awaiting a value
  // (the previous slot in the sequence just got picked) — react to that
  // transition and pop the picker open without another click.
  useEffect(() => {
    if (autoOpen) setOpen(true);
  }, [autoOpen]);
  const matches = columnsForSlot(columns, slot, excludeColumns);
  return (
    <span
      className={cn("inline-flex items-center gap-1 rounded border border-dashed px-2 py-0.5 text-xs font-mono", colorClass)}
    >
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <button
            type="button"
            className={cn("cursor-pointer hover:underline focus:outline-none", !value && "text-muted-foreground font-sans")}
          >
            {value || t("monitoredTables.selectColumnPlaceholder")}
          </button>
        </PopoverTrigger>
        <PopoverContent className="w-80 p-0" align="start">
          <ColumnDropdownList
            slot={slot}
            matches={matches}
            totalAll={columns.length}
            onSelect={(colName) => {
              onSelect(colName);
              setOpen(false);
            }}
          />
        </PopoverContent>
      </Popover>
      {onCancel && (
        <button
          type="button"
          onClick={onCancel}
          title={t("common.cancel")}
          aria-label={t("common.cancel")}
          className="ml-0.5 opacity-60 hover:opacity-100 focus:outline-none leading-none"
        >
          &times;
        </button>
      )}
    </span>
  );
}

/**
 * `cardinality: "many"` variant of `PendingSlotChip` — a slot bound to
 * several columns at once can't auto-advance on a single click (there's no
 * signal for "the multi-select is done"), so it keeps one explicit confirm
 * step. This is a legacy, uncommon slot shape (see
 * `lib/registry-rule-conversion.ts`); every `cardinality: "one"` slot (the
 * common case, including single-slot rules) gets the fully one-click flow
 * above.
 */
function PendingManySlotChip({
  slot,
  columns,
  excludeColumns,
  onCommit,
}: {
  slot: RuleSlot;
  columns: ColumnOut[];
  excludeColumns: string[];
  onCommit: (colNames: string[]) => void;
}) {
  const { t } = useTranslation();
  const [selected, setSelected] = useState<string[]>([]);
  return (
    <div className="flex flex-col gap-1.5 w-full max-w-xs">
      <MultiColumnPicker slot={slot} columns={columns} value={selected} onChange={setSelected} excludeColumns={excludeColumns} />
      <div className="flex justify-end">
        <Button size="sm" variant="secondary" disabled={selected.length === 0} onClick={() => onCommit(selected)}>
          {t("monitoredTables.applyButton")}
        </Button>
      </div>
    </div>
  );
}

interface MappingChipsProps {
  /** One mapping GROUP per materialized check: slot-name -> column-name. */
  columnMapping: AppliedRuleOutColumnMappingItem[];
  /** Declared slots for the rule. When provided, chips render per-slot rows
   *  (matching dqlake's layout: `{{slot}}` + family badge -> chips). When
   *  omitted, falls back to one flat chip row per mapping group. */
  slots?: RuleSlot[];
  /** The table's real columns — required to render editable chips
   *  (`onChangeGroup`); a read-only chip list (e.g. the AI-suggestion
   *  preview) can omit it. */
  columns?: ColumnOut[];
  /** Jump to a column's card in the by-column lens. */
  onJumpToColumn?: (colName: string) => void;
  /** Reassigns the column for one slot within one mapping group — makes
   *  every chip clickable (opens a column-picker popover) instead of a
   *  static label. Omit to render non-interactive chips. */
  onChangeGroup?: (groupIdx: number, slotName: string, colName: string) => void;
  /** Removes the mapping group at this index (every chip for that group,
   *  across every slot row). Omit to render fully read-only chips with no
   *  remove affordance. */
  onRemoveGroup?: (groupIdx: number) => void;
  /** Mapping-group index currently being removed — its remove ("x")
   *  affordance shows a spinner and is disabled while the request is
   *  in flight. */
  busyGroupIdx?: number | null;
  /** Starts the "apply this rule to another column" flow. Rendered as a
   *  dashed "+ Apply to another column" button on the last slot row. Omit
   *  to hide it (also omit while a flow is already in progress — see
   *  `pendingValues`). */
  onAddGroup?: () => void;
  /** Values chosen so far for an in-progress "add mapping group" flow,
   *  keyed by slot name (comma-joined for `cardinality: "many"` slots,
   *  matching `column_mapping`'s own storage shape). Presence (even `{}`)
   *  means the flow is active: each slot row renders an inline placeholder
   *  chip instead of the "+ Apply to another column" button, the next
   *  not-yet-filled slot's picker auto-opens, and once every slot has a
   *  value the caller's `onPendingSelect` handler is expected to commit the
   *  group (via `onAddMapping`) and clear this back to `undefined`. */
  pendingValues?: Record<string, string>;
  /** Commits a value for one slot of the in-progress group described by
   *  `pendingValues`. Purely reports "slot X now has value Y" — the caller
   *  owns deciding when the group is complete and staging it. */
  onPendingSelect?: (slotName: string, colName: string) => void;
  /** Aborts an in-progress "add mapping group" flow. Rendered as a small
   *  "x" on the last slot row's pending chip. */
  onCancelAdd?: () => void;
  className?: string;
}

export function MappingChips({
  columnMapping,
  slots,
  columns = [],
  onJumpToColumn,
  onChangeGroup,
  onRemoveGroup,
  busyGroupIdx = null,
  onAddGroup,
  pendingValues,
  onPendingSelect,
  onCancelAdd,
  className,
}: MappingChipsProps) {
  const { t } = useTranslation();
  const addingGroup = pendingValues !== undefined;
  if (columnMapping.length === 0 && !onAddGroup && !addingGroup) return null;

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
  // Every column already used anywhere in this rule's mapping — excluded
  // from a chip's own picker candidates (minus its own current value, see
  // `EditableChip`) so the same column can't be picked twice across groups.
  const usedColumns = columnMapping.flatMap((group) => slotNames.map((s) => group[s]).filter((v): v is string => Boolean(v)));
  // The next slot in declaration order still missing a value in the
  // in-progress group — its picker auto-opens as soon as it becomes this,
  // so filling one `cardinality: "one"` slot immediately reveals the next
  // one's column list with no extra click to open it.
  const activePendingSlot = addingGroup ? slots.find((s) => !pendingValues?.[s.name])?.name : undefined;

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
                {filled.length === 0 && !onAddGroup && !addingGroup ? (
                  <span className="text-xs text-muted-foreground italic">
                    {t("monitoredTables.noColumnMapped")}
                  </span>
                ) : (
                  filled.map(({ colName, groupIdx }) =>
                    onChangeGroup ? (
                      <EditableChip
                        key={groupIdx}
                        colorClass={paletteAt(groupIdx)}
                        label={colName}
                        slot={slot}
                        columns={columns}
                        excludeColumns={usedColumns}
                        onChange={(next) => onChangeGroup(groupIdx, slot.name, next)}
                        onJump={onJumpToColumn ? () => onJumpToColumn(colName) : undefined}
                        onRemove={onRemoveGroup ? () => onRemoveGroup(groupIdx) : undefined}
                        removeTitle={t("monitoredTables.removeMappingGroupTitle", { count: groupIdx + 1 })}
                      />
                    ) : (
                      <ReadonlyChip
                        key={groupIdx}
                        colorClass={paletteAt(groupIdx)}
                        label={colName}
                        onJump={onJumpToColumn ? () => onJumpToColumn(colName) : undefined}
                        onRemove={onRemoveGroup ? () => onRemoveGroup(groupIdx) : undefined}
                        removeTitle={t("monitoredTables.removeMappingGroupTitle", { count: groupIdx + 1 })}
                        busy={busyGroupIdx === groupIdx}
                      />
                    ),
                  )
                )}

                {/* In-progress "add mapping group" flow: one placeholder
                    chip per slot, rendered inline in that slot's own row —
                    no separate form/dialog. `cardinality: "one"` slots (the
                    common case, including every single-slot rule) commit
                    and auto-advance to the next slot's picker on a single
                    click; `cardinality: "many"` keeps one explicit confirm
                    step since a multi-select has no "I'm done" signal. */}
                {addingGroup &&
                  onPendingSelect &&
                  (() => {
                    // Exclude columns already picked for *other* slots in
                    // this same in-progress group — same column can't fill
                    // two slots of one mapping group.
                    const pendingSiblingColumns = Object.entries(pendingValues ?? {})
                      .filter(([slotName]) => slotName !== slot.name)
                      .flatMap(([, v]) => v.split(","));
                    const excludeColumns = [...usedColumns, ...pendingSiblingColumns];
                    return slot.cardinality === "many" ? (
                      <PendingManySlotChip
                        key="pending"
                        slot={slot}
                        columns={columns}
                        excludeColumns={excludeColumns}
                        onCommit={(colNames) => onPendingSelect(slot.name, colNames.join(","))}
                      />
                    ) : (
                      <PendingSlotChip
                        key="pending"
                        slot={slot}
                        columns={columns}
                        excludeColumns={excludeColumns}
                        value={pendingValues?.[slot.name]}
                        autoOpen={slot.name === activePendingSlot}
                        colorClass={paletteAt(columnMapping.length)}
                        onSelect={(colName) => onPendingSelect(slot.name, colName)}
                        onCancel={isLastSlot ? onCancelAdd : undefined}
                      />
                    );
                  })()}

                {/* + Apply to another column: only on the last slot row */}
                {onAddGroup && isLastSlot && !addingGroup && (
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
