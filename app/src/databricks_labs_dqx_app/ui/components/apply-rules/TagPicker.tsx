// TagPicker — searchable list of Unity Catalog governed tags.
//
// Fetches real tags via `useListGovernedTags` (lazy — only mounts when the
// popover is open). Filters out already-selected tags and applies a
// case-insensitive substring search. No free-text entry — the input only
// narrows the real tag list.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Loader2 } from "lucide-react";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useListGovernedTags } from "@/lib/api";
import selector from "@/lib/selector";
import { filterTags } from "./tag-picker-utils";

export interface TagPickerProps {
  selected: string[];
  onSelect: (tag: string) => void;
}

/** Popover body: search box + scrollable list of available governed tags.
 *  Mounts only when the popover opens, so the fetch is naturally lazy. */
export function TagPicker({ selected, onSelect }: TagPickerProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");

  const { data, isLoading, isError } = useListGovernedTags(selector());

  const governedTags = data?.tags ?? [];
  const allTags: string[] = governedTags.map((g) => g.tag);
  const descriptions = new Map<string, string | null>(
    governedTags.map((g) => [g.tag, g.description ?? null]),
  );
  const visible = filterTags(allTags, selected, search);

  return (
    <TooltipProvider delayDuration={200}>
    <div className="p-0">
      <Command shouldFilter={false}>
        <CommandInput
          value={search}
          onValueChange={setSearch}
          placeholder={t("monitoredTables.tagPickerSearchPlaceholder")}
          className="h-9 text-xs"
        />
        <CommandList className="max-h-64">
          {isLoading && (
            <div className="flex items-center gap-2 px-3 py-2 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
              {t("monitoredTables.tagPickerLoading")}
            </div>
          )}
          {isError && !isLoading && (
            <div className="px-3 py-2 text-xs text-muted-foreground">
              {t("monitoredTables.tagPickerError")}
            </div>
          )}
          {!isLoading && !isError && (
            <>
              {visible.length === 0 ? (
                <CommandEmpty>
                  <span className="text-xs text-muted-foreground">
                    {t("monitoredTables.tagPickerEmpty")}
                  </span>
                </CommandEmpty>
              ) : (
                <CommandGroup>
                  {visible.map((tag) => {
                    const description = descriptions.get(tag);
                    const item = (
                      <CommandItem
                        value={tag}
                        onSelect={() => onSelect(tag)}
                        className="text-xs font-mono"
                      >
                        {tag}
                      </CommandItem>
                    );
                    // App-native tooltip with the governed-tag description, shown
                    // to the side so it doesn't cover the list. Only rendered when
                    // the tag actually has a description (many are NULL in UC);
                    // otherwise the bare CommandItem is returned so cmdk's item
                    // registration/keyboard-nav is never wrapped in extra DOM.
                    if (!description) return <CommandItem key={tag} value={tag} onSelect={() => onSelect(tag)} className="text-xs font-mono">{tag}</CommandItem>;
                    return (
                      <Tooltip key={tag}>
                        <TooltipTrigger asChild>{item}</TooltipTrigger>
                        <TooltipContent side="right" className="max-w-xs">
                          {description}
                        </TooltipContent>
                      </Tooltip>
                    );
                  })}
                </CommandGroup>
              )}
            </>
          )}
        </CommandList>
      </Command>
    </div>
    </TooltipProvider>
  );
}
