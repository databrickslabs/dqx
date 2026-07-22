import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { ChevronDown, Loader2 } from "lucide-react";
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
  /** When true, behaves as a single-select: no checkboxes, no select-all,
   *  selecting an item always replaces the selection and closes the popover. */
  single?: boolean;
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
  single = false,
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

  const closeAndClearSearch = () => {
    setOpen(false);
    setSearch("");
  };

  // Row-click convention: clicking the row LABEL (as opposed to its
  // checkbox — see the item's `onClick` below) is a "pick this one and
  // move on" gesture, so it replaces the selection outright and closes the
  // popover — the common single-pick case shouldn't require a second click
  // to dismiss the dropdown. It only falls back to plain checkbox-style
  // toggle-and-stay-open once the user is clearly building a multi-item
  // selection (more than one already picked), so extending an existing
  // multi-select by row-clicking doesn't collapse it down to one. Keyboard
  // Enter goes through this same handler (cmdk's `onSelect` fires for both
  // click and Enter), so the two stay consistent by construction.
  // In `single` mode this always replaces-and-closes regardless of how many
  // items are already selected.
  const selectRow = (o: MultiSelectOption) => {
    if (o.disabled) return;
    if (!single && selected.length > 1) {
      toggle(o);
      return;
    }
    onChange([o.value]);
    closeAndClearSearch();
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
      {/* TRUE ROOT CAUSE of "still can't scroll" (verified in a real browser
          with mouse.wheel — no amount of CSS height-bounding below fixes
          this on its own): this picker is opened from inside a modal
          `Dialog` (AddMonitoredTableModal / ApplyRuleModal). Radix's Dialog
          locks page scroll via `react-remove-scroll`, which attaches a
          `document`-level wheel/touch listener that PREVENTS every wheel
          event whose target isn't a DOM descendant of the Dialog's own
          content node (or an explicit `shards` entry). This Popover's
          content renders into its own `Portal` — a sibling of the Dialog's
          portal in `document.body`, not a descendant — so every wheel event
          inside it was being swallowed by the Dialog's scroll lock, no
          matter how the list itself was sized. `modal` here makes the
          Popover wrap its own content in a *second*, nested
          `react-remove-scroll` lock (see `@radix-ui/react-popover`'s
          `PopoverContentModal`); its internal `lockStack` mechanism makes
          the most-recently-opened lock authoritative, so once this Popover
          is open its own lock evaluates wheel/touch events against its own
          (genuinely scrollable) content instead of deferring to the outer
          Dialog's now-irrelevant one. Confirmed via Playwright
          `page.mouse.wheel()` (a real, trusted wheel event) before/after:
          `scrollTop` stayed 0 without `modal`, advanced correctly with it. */}
      <Popover
        modal
        open={open}
        onOpenChange={(next) => (next ? setOpen(true) : closeAndClearSearch())}
      >
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
          {/* `shouldFilter={false}` — this list preserves its own semantics
              (substring match on `label`, selected-options-float-to-top
              ordering, tri-state select-all, disabled/already-monitored rows)
              via `filteredOptions` above rather than cmdk's fuzzy scoring. */}
          <Command shouldFilter={false} className="flex-1 min-h-0 overflow-hidden">
            {/* Search row: the shared Input primitive (used by `CommandInput`)
                bakes in its own `dark:bg-input/30` fill (see
                components/ui/input.tsx), which — because a `.dark` compound
                selector always outranks a bare `.bg-transparent` class
                regardless of source order — wins over the `bg-transparent`
                override and only tints the input's own rectangle. That's why
                the dark-mode search bar looked like a lighter-grey patch
                covering part of the row (the input) sitting on a darker row
                (the icon gutter + padding around it). Applying the same
                token to the row container makes the fill span the row's full
                width uniformly. */}
            <CommandInput
              value={search}
              onValueChange={setSearch}
              placeholder={searchPlaceholder}
              className="h-9 text-sm dark:bg-input/30"
            />
            {/* ROOT CAUSE of the "list still doesn't scroll" bug (see git
                history): the list needs its own bounded height in addition
                to the trigger's dynamic available-height cap — `min-h-0` on
                the flex child is required because flex items default to a
                min-height of their own content size, which would otherwise
                stop the list from ever shrinking below its unscrolled height
                even inside a bounded parent. */}
            <CommandList className="dq-scroll-visible min-h-0 max-h-[300px] flex-1 p-1">
              {isLoading ? (
                <div className="flex items-center justify-center py-6">
                  <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                </div>
              ) : (
                <>
                  <CommandEmpty>
                    <span className="text-xs text-muted-foreground">{emptyText}</span>
                  </CommandEmpty>
                  {filteredOptions.length > 0 && (
                    <CommandGroup>
                      {/* Select-all row is hidden in single mode — there's
                          nothing to multi-select and the affordance would be
                          confusing. */}
                      {!single && (
                        <CommandItem onSelect={toggleSelectAll} className="border-b rounded-none">
                          <Checkbox checked={selectAllState} className="shrink-0 pointer-events-none" />
                          <span className="text-xs text-muted-foreground">
                            {t("monitoredTables.wizard.selectAllVisible")}
                          </span>
                        </CommandItem>
                      )}
                      {filteredOptions.map((o) => {
                        const isSelected = selectedSet.has(o.value) || !!o.disabled;
                        // Interaction matrix (mouse and Enter are kept
                        // consistent — both end up calling `selectRow`,
                        // since cmdk fires `onSelect` for either):
                        //   - Click the checkbox itself (multi mode only):
                        //     always toggles that one option and keeps the
                        //     popover open — the explicit "building a
                        //     multi-selection" tool, so it never auto-closes.
                        //   - Click the row label / press Enter while it's
                        //     highlighted, with 0 or 1 option(s) currently
                        //     selected: replaces the selection with just this
                        //     option and closes the popover — the common
                        //     single-pick case shouldn't need a second
                        //     dismissal click.
                        //   - Same row-label/Enter gesture, but >1 option
                        //     already selected (multi mode only): falls back
                        //     to plain toggle-and-stay-open (matches dqlake's
                        //     multi-select `Command` behavior).
                        //   - In `single` mode: always replaces-and-closes
                        //     regardless of current selection count.
                        // Disabled rows are unselectable via keyboard, click,
                        // or the checkbox — cmdk skips `disabled` items during
                        // arrow-key navigation.
                        const item = (
                          <CommandItem
                            key={o.value}
                            value={o.value}
                            disabled={o.disabled}
                            onSelect={() => selectRow(o)}
                            className={cn(o.disabled ? "opacity-70" : undefined, isSelected && "bg-primary/10")}
                          >
                            {/* Checkboxes are hidden in single mode — the
                                radio-style highlight is enough visual feedback. */}
                            {!single && (
                              <Checkbox
                                checked={isSelected}
                                disabled={o.disabled}
                                className="shrink-0"
                                onClick={(e) => {
                                  // Stops the click from bubbling to the
                                  // CommandItem's own `onClick` (cmdk's
                                  // selection trigger) — the checkbox handles
                                  // its own toggle-and-stay-open semantics
                                  // instead of the row's close-on-single-pick
                                  // behavior above.
                                  e.stopPropagation();
                                  toggle(o);
                                }}
                              />
                            )}
                            <span className="truncate text-sm" title={o.label}>
                              {o.label}
                            </span>
                          </CommandItem>
                        );
                        if (!o.disabled || !o.disabledReason) return item;
                        return (
                          <Tooltip key={o.value}>
                            <TooltipTrigger asChild>{item}</TooltipTrigger>
                            <TooltipContent side="right">{o.disabledReason}</TooltipContent>
                          </Tooltip>
                        );
                      })}
                    </CommandGroup>
                  )}
                </>
              )}
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  );
}
