import { useMemo, useState } from "react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Check, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { FILTER_TRIGGER_CLASS } from "@/components/data-table/filter-bar";

export interface SearchableSelectOption {
  value: string;
  label: string;
}

interface SearchableSelectProps {
  value: string;
  onChange: (value: string) => void;
  options: SearchableSelectOption[];
  /** Label for the sentinel "all / no filter" option, always shown first and
   *  never filtered out (e.g. "All stewards"). */
  allLabel: string;
  /** Sentinel value representing "no filter". Defaults to `"all"`. */
  allValue?: string;
  searchPlaceholder: string;
  emptyText: string;
  /** Accessible name for the trigger button. */
  ariaLabel?: string;
  /** Width/height token for the trigger; defaults to the shared filter-bar
   *  token so it lines up with the adjacent plain `Select` filters. */
  className?: string;
  disabled?: boolean;
}

/**
 * Single-select filter control with a type-to-search box, for overview
 * filter bars whose option lists (catalogs, schemas, stewards) can grow long
 * enough that a plain scrolling `Select` is awkward (P25 item 57). Built on
 * the shared Popover + cmdk `Command` primitives — the same building blocks as
 * `MultiSelectPopover` / `Labels.tsx`'s `SearchPickerPopover` — and styled to
 * match the neighbouring `SelectTrigger` so a bar can freely mix the two.
 *
 * Filter semantics are identical to the `Select` it replaces: the sentinel
 * "all" option resets the filter, and picking any option sets that value.
 */
export function SearchableSelect({
  value,
  onChange,
  options,
  allLabel,
  allValue = "all",
  searchPlaceholder,
  emptyText,
  ariaLabel,
  className,
  disabled,
}: SearchableSelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");

  const selectedLabel = useMemo(() => {
    if (value === allValue) return allLabel;
    return options.find((o) => o.value === value)?.label ?? value;
  }, [value, allValue, allLabel, options]);

  // `shouldFilter={false}` — own substring match on the label, so cmdk's
  // fuzzy scoring doesn't reorder the (already sorted) option list. The "all"
  // reset row is always shown regardless of the query.
  const filteredOptions = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return options;
    return options.filter((o) => o.label.toLowerCase().includes(q));
  }, [options, search]);

  const select = (next: string) => {
    onChange(next);
    setOpen(false);
    setSearch("");
  };

  return (
    <Popover open={open} onOpenChange={(next) => (next ? setOpen(true) : (setOpen(false), setSearch("")))}>
      <PopoverTrigger asChild>
        <button
          type="button"
          role="combobox"
          aria-expanded={open}
          aria-label={ariaLabel}
          disabled={disabled}
          className={cn(
            "border-input dark:bg-input/30 dark:hover:bg-input/50 focus-visible:border-ring focus-visible:ring-ring/50 flex items-center justify-between gap-2 rounded-md border bg-transparent px-3 py-1 whitespace-nowrap shadow-xs transition-[color,box-shadow] outline-none focus-visible:ring-[3px] disabled:cursor-not-allowed disabled:opacity-50",
            FILTER_TRIGGER_CLASS,
            className,
          )}
        >
          <span className="truncate">{selectedLabel}</span>
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
              <CommandItem value={allValue} onSelect={() => select(allValue)} className="text-xs">
                <Check className={cn("size-3.5", value === allValue ? "opacity-100" : "opacity-0")} />
                {allLabel}
              </CommandItem>
              {filteredOptions.map((o) => (
                <CommandItem key={o.value} value={o.value} onSelect={() => select(o.value)} className="text-xs">
                  <Check className={cn("size-3.5", value === o.value ? "opacity-100" : "opacity-0")} />
                  <span className="truncate" title={o.label}>
                    {o.label}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
