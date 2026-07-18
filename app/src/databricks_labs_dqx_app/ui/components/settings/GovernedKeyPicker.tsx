// GovernedKeyPicker — "Add new governed key" button + popover for the admin
// Custom tags settings card. Lists Unity Catalog GOVERNED tag KEYs (deduped to
// top-level keys via the same `groupTags` helper the apply-rules TagPicker
// uses) and, on select, seeds a new custom-tag draft pre-populated from that
// governed tag (key, description, allowed values). This is import-time
// population only — no ongoing sync. Keys already present as drafts are omitted
// from the list.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { BookMarked, Loader2 } from "lucide-react";
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
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useListGovernedTags } from "@/lib/api";
import selector from "@/lib/selector";
import { groupTags } from "@/components/apply-rules/tag-picker-utils";

/** The bits of a governed KEY needed to seed a custom-tag draft. */
export interface GovernedKeySeed {
  key: string;
  description: string;
  values: string[];
}

export interface GovernedKeyPickerProps {
  /** Custom-tag draft keys already present — omitted from the picker. */
  existingKeys: string[];
  /** Fires with the seed for the picked governed KEY; the parent creates the draft. */
  onPick: (seed: GovernedKeySeed) => void;
}

/** Outline button matching "Add new key" that opens a searchable list of
 *  governed KEYs. Mounts the query lazily (only once the popover opens). */
export function GovernedKeyPicker({ existingKeys, onPick }: GovernedKeyPickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="gap-1.5">
          <BookMarked className="h-3.5 w-3.5" />
          {t("config.addGovernedKey")}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80 p-0" align="start">
        {open && (
          <GovernedKeyList
            existingKeys={existingKeys}
            onPick={(seed) => {
              onPick(seed);
              setOpen(false);
            }}
          />
        )}
      </PopoverContent>
    </Popover>
  );
}

/** Popover body — split out so the governed-tags query mounts only when open. */
function GovernedKeyList({ existingKeys, onPick }: GovernedKeyPickerProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const { data, isLoading, isError } = useListGovernedTags(selector());

  const governedTags = data?.tags ?? [];
  const allTags: string[] = governedTags.map((g) => g.tag);
  // Description keyed by the bare governed KEY (many are NULL in UC).
  const descriptions = new Map<string, string | null>(
    governedTags.map((g) => [g.tag, g.description ?? null]),
  );

  const existingSet = new Set(existingKeys.map((k) => k.trim()));
  // `groupTags([], allTags)` collapses every `KEY` / `KEY=value` variant into
  // one group per top-level KEY, exposing its full list of allowed values.
  const groups = groupTags(allTags, []).filter((g) => !existingSet.has(g.key));

  const q = search.trim().toLowerCase();
  const visible = q === "" ? groups : groups.filter((g) => g.key.toLowerCase().includes(q));

  return (
    <TooltipProvider delayDuration={200}>
      <Command shouldFilter={false}>
        <CommandInput
          value={search}
          onValueChange={setSearch}
          placeholder={t("config.governedKeySearchPlaceholder")}
          className="h-9 text-xs"
        />
        <CommandList className="max-h-64">
          {isLoading && (
            <div className="flex items-center gap-2 px-3 py-2 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
              {t("config.governedKeyLoading")}
            </div>
          )}
          {isError && !isLoading && (
            <div className="px-3 py-2 text-xs text-muted-foreground">
              {t("config.governedKeyError")}
            </div>
          )}
          {!isLoading && !isError && (
            <>
              {visible.length === 0 ? (
                <CommandEmpty>
                  <span className="text-xs text-muted-foreground">
                    {t("config.governedKeyEmpty")}
                  </span>
                </CommandEmpty>
              ) : (
                <CommandGroup>
                  {visible.map((group) => {
                    const description = descriptions.get(group.key) ?? "";
                    const item = (
                      <CommandItem
                        key={group.key}
                        value={group.key}
                        onSelect={() =>
                          onPick({ key: group.key, description, values: group.values })
                        }
                        className="items-center gap-2 text-xs font-mono"
                      >
                        <span className="min-w-0 flex-1 truncate">{group.key}</span>
                        {group.values.length > 0 && (
                          <span className="shrink-0 text-[10px] text-muted-foreground">
                            {t("config.governedKeyValueCount", { count: group.values.length })}
                          </span>
                        )}
                      </CommandItem>
                    );
                    if (!description) return item;
                    return (
                      <Tooltip key={group.key}>
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
    </TooltipProvider>
  );
}
