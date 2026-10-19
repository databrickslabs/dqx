import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
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
import { catalogMatches } from "@/lib/setup-catalogs";
import { cn } from "@/lib/utils";

type SetupCatalogPickerProps = {
  id: string;
  value: string;
  catalogs: readonly string[];
  isLoading: boolean;
  onChange: (catalog: string) => void;
};

/**
 * Searchable single-select catalog picker for the setup form. Follows the
 * single mode of MultiSelectPopover, and also offers the typed name as an
 * option so a catalog the list doesn't show can still be chosen.
 */
export function SetupCatalogPicker({
  id,
  value,
  catalogs,
  isLoading,
  onChange,
}: SetupCatalogPickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const { matches, typed } = useMemo(
    () => catalogMatches(catalogs, search),
    [catalogs, search],
  );

  const close = () => {
    setOpen(false);
    setSearch("");
  };
  const choose = (catalog: string) => {
    onChange(catalog);
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
          disabled={isLoading}
          className="w-full justify-between font-normal"
        >
          <span className={cn("truncate", !value && "text-muted-foreground")}>
            {value || t("setup.configuration.catalogPlaceholder")}
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
        {/* Filtering happens in catalogMatches, so cmdk's own fuzzy scoring is off. */}
        <Command shouldFilter={false} className="flex-1 min-h-0 overflow-hidden">
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={t("setup.configuration.catalogSearch")}
            className="h-9 text-sm dark:bg-input/30"
          />
          <CommandList className="dq-scroll-visible min-h-0 max-h-[300px] flex-1 p-1">
            <CommandGroup>
              {typed && (
                <CommandItem value={`typed:${typed}`} onSelect={() => choose(typed)}>
                  <span className="truncate text-sm" title={typed}>
                    {t("setup.configuration.catalogUseTyped", { name: typed })}
                  </span>
                </CommandItem>
              )}
              {matches.map((catalog) => (
                <CommandItem
                  key={catalog}
                  value={catalog}
                  onSelect={() => choose(catalog)}
                  className={cn(catalog === value && "bg-primary/10")}
                >
                  <span className="truncate text-sm" title={catalog}>
                    {catalog}
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
