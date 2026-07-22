import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Checkbox } from "@/components/ui/checkbox";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { FILTER_TRIGGER_CLASS } from "@/components/data-table/filter-bar";

export interface FacetFilterOption {
  value: string;
  label: string;
}

/**
 * A compact, searchable MULTI-select facet dropdown for the top-level Global
 * Results filter row (item 35). Styled to match the overview pages' filter
 * bubbles — it reuses the shared `FILTER_TRIGGER_CLASS` trigger token (the
 * same one `SearchableSelect` uses) and the same Popover + cmdk `Command`
 * primitives as `MultiSelectPopover`, so the Global Results filters read as
 * the same control family as the Rules / Tables / Spaces filter bars.
 *
 * Multi-select: each option toggles in/out of the `selected` set; the trigger
 * shows the sole option's label for a single pick, a count for several, and
 * the `allLabel` sentinel when nothing is selected.
 */
export function ResultsFacetFilter({
  allLabel,
  options,
  selected,
  onChange,
  searchPlaceholder,
  emptyText,
  ariaLabel,
  className,
}: {
  /** Trigger text + first (reset) row label when nothing is selected. */
  allLabel: string;
  options: FacetFilterOption[];
  selected: string[];
  onChange: (values: string[]) => void;
  searchPlaceholder: string;
  emptyText: string;
  ariaLabel?: string;
  className?: string;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");

  const selectedSet = useMemo(() => new Set(selected), [selected]);

  const triggerLabel = useMemo(() => {
    if (selected.length === 0) return allLabel;
    if (selected.length === 1) {
      return options.find((o) => o.value === selected[0])?.label ?? selected[0];
    }
    return t("resultsUi.filterSelectedCount", { count: selected.length });
  }, [selected, options, allLabel, t]);

  // Own substring match on the label (`shouldFilter={false}`) so cmdk's fuzzy
  // scoring doesn't reorder the already-sorted option list.
  const filteredOptions = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return options;
    return options.filter((o) => o.label.toLowerCase().includes(q));
  }, [options, search]);

  const toggle = (value: string) => {
    if (selectedSet.has(value)) {
      onChange(selected.filter((v) => v !== value));
    } else {
      onChange([...selected, value]);
    }
  };

  return (
    <Popover open={open} onOpenChange={(next) => (next ? setOpen(true) : (setOpen(false), setSearch("")))}>
      <PopoverTrigger asChild>
        <button
          type="button"
          role="combobox"
          aria-expanded={open}
          aria-label={ariaLabel}
          className={cn(
            "border-input dark:bg-input/30 dark:hover:bg-input/50 focus-visible:border-ring focus-visible:ring-ring/50 flex items-center justify-between gap-2 rounded-md border bg-transparent px-3 py-1 whitespace-nowrap shadow-xs transition-[color,box-shadow] outline-none focus-visible:ring-[3px] disabled:cursor-not-allowed disabled:opacity-50",
            FILTER_TRIGGER_CLASS,
            selected.length > 0 && "border-ring/60 text-foreground",
            className,
          )}
        >
          <span className="truncate">{triggerLabel}</span>
          <ChevronDown className="size-4 shrink-0 opacity-50" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-(--radix-popover-trigger-width) p-0">
        <Command shouldFilter={false}>
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={searchPlaceholder}
            className="h-8 text-xs"
          />
          <CommandList className="max-h-56">
            <CommandEmpty>
              <span className="text-xs text-muted-foreground">{emptyText}</span>
            </CommandEmpty>
            <CommandGroup>
              {filteredOptions.map((o) => {
                const isSelected = selectedSet.has(o.value);
                return (
                  <CommandItem
                    key={o.value}
                    value={o.value}
                    onSelect={() => toggle(o.value)}
                    className={cn("text-xs", isSelected && "bg-primary/10")}
                  >
                    <Checkbox checked={isSelected} className="shrink-0 pointer-events-none" />
                    <span className="truncate" title={o.label}>
                      {o.label}
                    </span>
                  </CommandItem>
                );
              })}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
