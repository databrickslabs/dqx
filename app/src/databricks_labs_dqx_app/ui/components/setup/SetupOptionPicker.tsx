import { useMemo, useState } from "react";
import { ChevronDown, Loader2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Command,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { optionMatches } from "@/lib/setup-options";
import { cn } from "@/lib/utils";

type SetupOptionPickerProps = {
  id: string;
  value: string;
  options: readonly string[];
  /** Still listing: the admin can already type a name, the list fills in behind it. */
  isLoading: boolean;
  placeholder: string;
  searchPlaceholder: string;
  /** Text of the row that chooses the typed name when it isn't listed. */
  typedLabel: (name: string) => string;
  onChange: (option: string) => void;
};

/**
 * Searchable single-select picker for the setup form (catalogs, groups). Follows
 * the single mode of MultiSelectPopover, and also offers the typed name as an
 * option so a name the list doesn't show can still be chosen.
 */
export function SetupOptionPicker({
  id,
  value,
  options,
  isLoading,
  placeholder,
  searchPlaceholder,
  typedLabel,
  onChange,
}: SetupOptionPickerProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const { matches, typed } = useMemo(
    () => optionMatches(options, search),
    [options, search],
  );

  const close = () => {
    setOpen(false);
    setSearch("");
  };
  const choose = (option: string) => {
    onChange(option);
    close();
  };

  return (
    <Popover
      open={open}
      onOpenChange={(next) => (next ? setOpen(true) : close())}
    >
      <PopoverTrigger asChild>
        <Button
          id={id}
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          aria-busy={isLoading}
          className="w-full justify-between font-normal"
        >
          <span className={cn("truncate", !value && "text-muted-foreground")}>
            {value || placeholder}
          </span>
          {isLoading ? (
            <Loader2 className="ml-2 h-4 w-4 shrink-0 animate-spin opacity-50" />
          ) : (
            <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="flex max-h-(--radix-popover-content-available-height) w-[--radix-popover-trigger-width] flex-col overflow-hidden p-0"
        align="start"
      >
        {/* Filtering happens in optionMatches, so cmdk's own fuzzy scoring is off. */}
        <Command shouldFilter={false} className="flex-1 min-h-0 overflow-hidden">
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={searchPlaceholder}
            className="h-9 text-sm dark:bg-input/30"
          />
          <CommandList className="dq-scroll-visible min-h-0 max-h-[300px] flex-1 p-1">
            <CommandGroup>
              {typed && (
                <CommandItem value={`typed:${typed}`} onSelect={() => choose(typed)}>
                  <span className="truncate text-sm" title={typed}>
                    {typedLabel(typed)}
                  </span>
                </CommandItem>
              )}
              {matches.map((option) => (
                <CommandItem
                  key={option}
                  value={option}
                  onSelect={() => choose(option)}
                  className={cn(option === value && "bg-primary/10")}
                >
                  <span className="truncate text-sm" title={option}>
                    {option}
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
