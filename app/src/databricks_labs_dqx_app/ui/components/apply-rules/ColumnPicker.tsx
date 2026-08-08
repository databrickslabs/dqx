// ColumnPicker — family-filtered column selector used when mapping a
// registry rule's slots to real table columns. A slot's `family` ("numeric",
// "text", "temporal", "boolean", or "any") narrows the candidate list so
// users aren't offered columns that can't satisfy the check.
//
// The single-column picker's popover (trigger chip, search box, "Suggested"
// vs "Compatible columns" grouping, click-to-select-and-close) is ported 1:1
// from dqlake's `bindings/MappingChips.tsx` `ColumnPicker` — same layout,
// same exact-match suggestion heuristic, same commit behavior, now on the
// same `cmdk`-backed `Command` primitive dqlake uses, so ArrowUp/ArrowDown +
// Enter + type-ahead work the same way here.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, Star } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { cn } from "@/lib/utils";
import type { ColumnOut, RuleSlot } from "@/lib/api";

export type ColumnFamily = "numeric" | "text" | "temporal" | "boolean" | "any";

export function familyForType(typeName: string): ColumnFamily {
  const t = typeName.toUpperCase();
  if (/^(INT|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|LONG|SHORT|BYTE)/.test(t)) return "numeric";
  if (/^(STRING|VARCHAR|CHAR)/.test(t)) return "text";
  if (/^(DATE|TIMESTAMP)/.test(t)) return "temporal";
  if (t === "BOOLEAN") return "boolean";
  return "any";
}

/**
 * @param excludeColumns Column names to omit from the candidate list — used
 * by the "+ Apply to another column" flow to hide columns the rule is
 * already mapped to, mirroring dqlake's `usedSetForNew` exclusion.
 */
export function columnsForSlot(
  columns: ColumnOut[],
  slot: Pick<RuleSlot, "family">,
  excludeColumns?: string[],
): ColumnOut[] {
  const exclude = excludeColumns?.length ? new Set(excludeColumns) : null;
  return columns.filter(
    (c) => (slot.family === "any" || familyForType(c.type_name) === slot.family) && !exclude?.has(c.name),
  );
}

// ---------------------------------------------------------------------------
// Exact-match "Suggested" heuristic — ported from dqlake's `ColumnPicker`.
// Strips common column-name suffixes before comparing so a slot like
// `amount` surfaces a column named `amount_col` as a suggestion.
// ---------------------------------------------------------------------------

const STRIP_SUFFIXES = ["_col", "_id", "_dt", "_ts", "_at"];

function stripSuffix(name: string): string {
  const lower = name.toLowerCase();
  for (const sfx of STRIP_SUFFIXES) {
    if (lower.endsWith(sfx)) {
      return lower.slice(0, lower.length - sfx.length);
    }
  }
  return lower;
}

function isExactMatch(slotName: string, colName: string): boolean {
  return stripSuffix(slotName) === stripSuffix(colName);
}

export interface ColumnDropdownListProps {
  slot: RuleSlot;
  /** Already family-filtered + exclusion-filtered candidate columns. */
  matches: ColumnOut[];
  /** Total column count on the table, for the filter summary line. */
  totalAll: number;
  onSelect: (colName: string) => void;
}

/** Popover body: search box + "Suggested"/"Compatible columns" grouping —
 *  ported 1:1 (layout, grouping, exact-match heuristic) from dqlake's
 *  `bindings/MappingChips.tsx` `ColumnPicker`, now on the `Command`
 *  primitive so it keeps our own exact-match ordering (`shouldFilter=false`
 *  + a manual substring filter) instead of cmdk's fuzzy scoring. */
export function ColumnDropdownList({ slot, matches, totalAll, onSelect }: ColumnDropdownListProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");

  const matchSearch = (c: ColumnOut) => search === "" || c.name.toLowerCase().includes(search.toLowerCase());
  const suggested = useMemo(() => matches.filter((c) => isExactMatch(slot.name, c.name)), [matches, slot.name]);
  const rest = useMemo(() => matches.filter((c) => !isExactMatch(slot.name, c.name)), [matches, slot.name]);
  const suggestedVisible = suggested.filter(matchSearch);
  const restVisible = rest.filter(matchSearch);
  const noMatches = suggestedVisible.length === 0 && restVisible.length === 0;

  return (
    <div className="p-0">
      <div className="flex items-center px-3 pt-2 pb-1 text-[11px] text-muted-foreground border-b">
        <span>
          {slot.family === "any"
            ? t("monitoredTables.columnPickerCountAny", { count: totalAll })
            : t("monitoredTables.columnPickerCountFamily", { shown: matches.length, total: totalAll, family: slot.family })}
        </span>
      </div>
      <Command shouldFilter={false}>
        <CommandInput
          value={search}
          onValueChange={setSearch}
          placeholder={t("monitoredTables.searchColumnsPlaceholder")}
          className="h-9 text-xs"
        />
        <CommandList className="max-h-64">
          {noMatches && (
            <CommandEmpty>
              <span className="text-xs text-muted-foreground">
                {matches.length === 0 ? t("monitoredTables.noMatchingColumns") : t("monitoredTables.noColumnsFound")}
              </span>
            </CommandEmpty>
          )}
          {suggestedVisible.length > 0 && (
            <CommandGroup heading={t("monitoredTables.suggestedColumnsHeading")}>
              {suggestedVisible.map((col) => (
                <CommandItem key={col.name} value={col.name} onSelect={() => onSelect(col.name)} className="text-xs">
                  <Star className="h-3 w-3 text-yellow-400 shrink-0" aria-hidden />
                  <span className="font-mono flex-1 truncate">{col.name}</span>
                  <span className="text-[10px] text-muted-foreground shrink-0">{col.type_name}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
          {restVisible.length > 0 && (
            <CommandGroup
              heading={suggestedVisible.length > 0 ? t("monitoredTables.compatibleColumnsHeading") : undefined}
            >
              {restVisible.map((col) => (
                <CommandItem key={col.name} value={col.name} onSelect={() => onSelect(col.name)} className="text-xs">
                  <span className="font-mono flex-1 truncate">{col.name}</span>
                  <span className="text-[10px] text-muted-foreground shrink-0">{col.type_name}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
        </CommandList>
      </Command>
    </div>
  );
}

interface SingleColumnPickerProps {
  slot: RuleSlot;
  columns: ColumnOut[];
  value: string | undefined;
  onChange: (column: string) => void;
  /** See `columnsForSlot`'s `excludeColumns`. */
  excludeColumns?: string[];
  /** Auto-open the popover on mount — mirrors dqlake's auto-open of a
   *  freshly added mapping-group placeholder chip right after
   *  "+ Apply to another column" is clicked, so the steward doesn't need an
   *  extra click on the newly revealed slot. */
  autoOpen?: boolean;
}

/** Single-column picker for a `cardinality: "one"` slot. Trigger is a
 *  combobox-style button; clicking it opens dqlake's column-picker popover
 *  (search + Suggested/Compatible columns). Selecting a row commits the
 *  value and closes the popover immediately — matching dqlake's
 *  click-to-select-and-commit behavior. */
export function SingleColumnPicker({ slot, columns, value, onChange, excludeColumns, autoOpen }: SingleColumnPickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(Boolean(autoOpen));
  const matches = columnsForSlot(columns, slot, excludeColumns);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="h-8 w-full justify-between text-xs font-mono font-normal"
        >
          <span className={cn("truncate", !value && "text-muted-foreground font-sans")}>
            {value || t("monitoredTables.selectColumnPlaceholder")}
          </span>
          <ChevronDown className="ml-2 h-3.5 w-3.5 shrink-0 opacity-50" aria-hidden />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80 p-0" align="start">
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
  );
}

interface MultiColumnPickerProps {
  slot: RuleSlot;
  columns: ColumnOut[];
  value: string[];
  onChange: (columns: string[]) => void;
  /** See `columnsForSlot`'s `excludeColumns`. */
  excludeColumns?: string[];
}

/** Multi-column checklist picker for a `cardinality: "many"` slot. */
export function MultiColumnPicker({ slot, columns, value, onChange, excludeColumns }: MultiColumnPickerProps) {
  const { t } = useTranslation();
  const matches = columnsForSlot(columns, slot, excludeColumns);
  if (matches.length === 0) {
    return <p className="text-xs text-muted-foreground">{t("monitoredTables.noMatchingColumns")}</p>;
  }
  return (
    <div className="grid grid-cols-2 gap-1.5 max-h-40 overflow-y-auto border rounded-md p-2">
      {matches.map((col) => {
        const checked = value.includes(col.name);
        return (
          <label key={col.name} className="flex items-center gap-1.5 text-xs">
            <Checkbox
              checked={checked}
              onCheckedChange={(v) => {
                onChange(v ? [...value, col.name] : value.filter((c) => c !== col.name));
              }}
            />
            <span className="font-mono truncate">{col.name}</span>
          </label>
        );
      })}
    </div>
  );
}
