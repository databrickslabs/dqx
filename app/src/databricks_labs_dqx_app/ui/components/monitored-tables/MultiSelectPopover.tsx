import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ChevronDown, Loader2, Search } from "lucide-react";
import { cn } from "@/lib/utils";

export interface MultiSelectOption {
  value: string;
  label: string;
  /** When true, the option is pre-selected and can't be toggled off — used
   *  to represent already-monitored tables in the "monitor table(s)"
   *  picker (matches dqlake's `TablePickerInline` `markMonitored` behavior). */
  disabled?: boolean;
  /** Tooltip shown on hover for a disabled option. */
  disabledReason?: string;
}

interface MultiSelectPopoverProps {
  label: string;
  placeholder: string;
  searchPlaceholder: string;
  options: MultiSelectOption[];
  selected: string[];
  onChange: (values: string[]) => void;
  isLoading?: boolean;
  disabled?: boolean;
  emptyText: string;
  disabledHint?: string;
}

/**
 * Searchable, checkbox-driven multi-select dropdown built on the shared
 * Popover primitive. Used by the monitored-tables add wizard to pick
 * multiple catalogs, schemas, or tables without the layout cramping or
 * inconsistent empty-state issues of the old single-select flow.
 */
export function MultiSelectPopover({
  label,
  placeholder,
  searchPlaceholder,
  options,
  selected,
  onChange,
  isLoading,
  disabled,
  emptyText,
  disabledHint,
}: MultiSelectPopoverProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");

  const selectedSet = useMemo(() => new Set(selected), [selected]);

  // When exactly one option is selected, the trigger shows that option's own
  // name instead of a generic "1 selected" — a single pick reads far better
  // as "acme_catalog" than as a count. Falls back to the count label for 0
  // or 2+ selections (and defensively if the value can't be resolved to an
  // option, e.g. stale selection).
  const singleSelectedLabel = useMemo(() => {
    if (selected.length !== 1) return null;
    return options.find((o) => o.value === selected[0])?.label ?? null;
  }, [selected, options]);

  // Selected options float to the top of the list (in their existing
  // relative order), then the rest — so a user scrolling back into an
  // already-large selection doesn't have to hunt for what they picked.
  const filteredOptions = useMemo(() => {
    const q = search.trim().toLowerCase();
    const base = q ? options.filter((o) => o.label.toLowerCase().includes(q)) : options;
    const selectedOnes = base.filter((o) => selectedSet.has(o.value));
    const restOnes = base.filter((o) => !selectedSet.has(o.value));
    return [...selectedOnes, ...restOnes];
  }, [options, search, selectedSet]);

  const toggle = (o: MultiSelectOption) => {
    if (o.disabled) return;
    if (selectedSet.has(o.value)) {
      onChange(selected.filter((v) => v !== o.value));
    } else {
      onChange([...selected, o.value]);
    }
  };

  // Tri-state "select all" driven off the currently filtered/visible set:
  // unchecked when none are selected, checked when every visible option is
  // selected, indeterminate otherwise. Clicking it toggles between
  // "select every visible option" and "clear every visible option".
  const allFilteredSelected =
    filteredOptions.length > 0 &&
    filteredOptions.every((o) => selectedSet.has(o.value) || !!o.disabled);
  const someFilteredSelected = filteredOptions.some((o) => selectedSet.has(o.value) || !!o.disabled);
  const selectAllState: boolean | "indeterminate" = allFilteredSelected
    ? true
    : someFilteredSelected
      ? "indeterminate"
      : false;

  const toggleSelectAll = () => {
    if (allFilteredSelected) {
      // Disabled (already-monitored) options can't be unchecked, even by
      // "select all" — only their non-disabled siblings get cleared.
      const filteredValues = new Set(filteredOptions.filter((o) => !o.disabled).map((o) => o.value));
      onChange(selected.filter((v) => !filteredValues.has(v)));
    } else {
      const merged = new Set([...selected, ...filteredOptions.map((o) => o.value)]);
      onChange([...merged]);
    }
  };

  return (
    <div className="grid gap-1.5">
      <Label className="text-xs text-muted-foreground">{label}</Label>
      <Popover open={open} onOpenChange={(next) => { setOpen(next); if (!next) setSearch(""); }}>
        <PopoverTrigger asChild>
          <Button
            type="button"
            variant="outline"
            role="combobox"
            aria-expanded={open}
            disabled={disabled || isLoading}
            title={disabled ? disabledHint : undefined}
            className="w-full justify-between font-normal"
          >
            <span className={cn("truncate", selected.length === 0 && "text-muted-foreground")}>
              {selected.length === 0
                ? placeholder
                : (singleSelectedLabel ?? t("monitoredTables.wizard.selectedCount", { count: selected.length }))}
            </span>
            {isLoading ? (
              <Loader2 className="ml-2 h-4 w-4 shrink-0 animate-spin opacity-50" />
            ) : (
              <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
            )}
          </Button>
        </PopoverTrigger>
        {/* ROOT CAUSE of the "list still doesn't scroll" bug: PopoverContent
            itself had no height bound of its own — only the item list below
            had a hard-coded `max-h-[300px]`. When the popover opened with
            less than ~300px of room between the trigger and the viewport
            edge (e.g. this modal sitting mid-page), Radix doesn't shrink
            content to fit; the whole card (header + select-all row + the
            fixed 300px list) rendered at its full intrinsic height and the
            excess was pushed past the visible viewport — the list's own
            overflow-y-auto scrollbar existed, but sat mostly off-screen,
            unreachable by wheel/trackpad. `SelectContent` in this same repo
            avoids exactly this by binding its own max-height to Radix's
            computed `--radix-popover-content-available-height` (the actual
            free space Radix already calculated); this does the same, and
            makes the card a `flex flex-col` so the header/select-all rows
            stay pinned (`shrink-0`) while the item list is the sole
            `min-h-0 flex-1` flex child — `min-h-0` is required because flex
            items default to a min-height of their own content size, which
            would otherwise stop the list from ever shrinking below its
            unscrolled height even inside a bounded parent. The list keeps
            its own `max-h-[300px] overflow-y-auto` so it still caps out at a
            sane size when plenty of room is available, but now that cap is
            never larger than what's actually on screen. */}
        <PopoverContent
          className="flex max-h-(--radix-popover-content-available-height) w-[--radix-popover-trigger-width] flex-col overflow-hidden p-0"
          align="start"
        >
          {/* Search row: the shared Input primitive bakes in its own
              `dark:bg-input/30` fill (see components/ui/input.tsx), which —
              because a `.dark` compound selector always outranks a bare
              `.bg-transparent` class regardless of source order — wins over
              the `bg-transparent` override passed below and only tints the
              input's own rectangle. That's why the dark-mode search bar
              looked like a lighter-grey patch covering part of the row
              (the input) sitting on a darker row (the icon gutter + padding
              around it). Applying the same token to the row container makes
              the fill span the row's full width uniformly. */}
          <div className="flex shrink-0 items-center gap-2 border-b px-3 dark:bg-input/30">
            <Search className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder={searchPlaceholder}
              className="h-9 flex-1 border-0 bg-transparent shadow-none focus-visible:ring-0 text-sm px-0"
            />
          </div>
          <label className="flex shrink-0 items-center gap-2 border-b px-3 py-1.5 cursor-pointer">
            <Checkbox
              checked={selectAllState}
              onCheckedChange={toggleSelectAll}
              disabled={filteredOptions.length === 0}
              className="shrink-0"
            />
            <span className="text-xs text-muted-foreground">
              {t("monitoredTables.wizard.selectAllVisible")}
            </span>
          </label>
          <div className="min-h-0 max-h-[300px] flex-1 overflow-y-auto p-1">
            {isLoading ? (
              <div className="flex items-center justify-center py-6">
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              </div>
            ) : filteredOptions.length === 0 ? (
              <p className="py-6 text-center text-xs text-muted-foreground">{emptyText}</p>
            ) : (
              filteredOptions.map((o) => {
                const isSelected = selectedSet.has(o.value) || !!o.disabled;
                // Clicking the row selects the option AND closes the popover
                // (single-pick-and-go). Clicking the checkbox itself toggles
                // the option but keeps the popover open, so multi-selecting
                // several options doesn't require reopening the dropdown
                // each time — the checkbox click's `stopPropagation` below
                // is what keeps it from also firing this row handler.
                const row = (
                  <div
                    key={o.value}
                    role="option"
                    aria-selected={isSelected}
                    onClick={() => {
                      if (o.disabled) return;
                      toggle(o);
                      setOpen(false);
                    }}
                    className={cn(
                      "flex items-center gap-2 rounded-sm px-3 py-1.5 text-sm transition-colors",
                      o.disabled ? "cursor-not-allowed opacity-70" : "cursor-pointer",
                      isSelected ? "bg-primary/10" : "hover:bg-muted",
                    )}
                  >
                    <span className="flex shrink-0" onClick={(e) => e.stopPropagation()}>
                      <Checkbox
                        checked={isSelected}
                        onCheckedChange={() => toggle(o)}
                        disabled={o.disabled}
                      />
                    </span>
                    <span className="truncate text-sm" title={o.label}>
                      {o.label}
                    </span>
                  </div>
                );
                if (!o.disabled || !o.disabledReason) return row;
                return (
                  <Tooltip key={o.value}>
                    <TooltipTrigger asChild>{row}</TooltipTrigger>
                    <TooltipContent side="right">{o.disabledReason}</TooltipContent>
                  </Tooltip>
                );
              })
            )}
          </div>
        </PopoverContent>
      </Popover>
    </div>
  );
}
