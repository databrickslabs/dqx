// TagPicker — two-level drilldown list of Unity Catalog governed tags.
//
// Fetches real tags via `useListGovernedTags` (lazy — only mounts when the
// popover is open). Governed tags follow a `KEY` or `KEY=value` convention;
// this picker collapses all variants of a KEY into a single root row. A KEY
// that has `=value` variants drills into a secondary view listing those
// values. `onSelect` always fires with the full tag string ("KEY" or
// "KEY=value") so the caller's contract is unchanged. No free-text entry —
// the input only narrows the real tag list.

import { useState, type ReactElement } from "react";
import { useTranslation } from "react-i18next";
import { ArrowLeft, ArrowRight, Loader2 } from "lucide-react";
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
import { filterGroups, groupTags } from "./tag-picker-utils";

export interface TagPickerProps {
  selected: string[];
  onSelect: (tag: string) => void;
}

/** Popover body: search box + scrollable list of available governed tags,
 *  with a two-level drilldown for `KEY=value` tags. Mounts only when the
 *  popover opens, so the fetch is naturally lazy. */
export function TagPicker({ selected, onSelect }: TagPickerProps) {
  const { t } = useTranslation();
  // `view` is null at the root; a key string once drilled into that key's values.
  const [view, setView] = useState<string | null>(null);
  const [rootSearch, setRootSearch] = useState("");
  const [valueSearch, setValueSearch] = useState("");

  const { data, isLoading, isError } = useListGovernedTags(selector());

  const governedTags = data?.tags ?? [];
  const allTags: string[] = governedTags.map((g) => g.tag);
  const descriptions = new Map<string, string | null>(
    governedTags.map((g) => [g.tag, g.description ?? null]),
  );

  const groups = groupTags(allTags, selected);
  const visibleGroups = filterGroups(groups, rootSearch);
  const activeGroup = view === null ? null : (groups.find((g) => g.key === view) ?? null);

  const openRoot = () => {
    setView(null);
    setValueSearch("");
  };

  /** Render an item's label, wrapping it in a side tooltip when the full tag
   *  string has a governed-tag description (many are NULL in UC). Returned bare
   *  otherwise so cmdk's item registration/keyboard-nav isn't wrapped in extra
   *  DOM. `fullTag` keys the description; `node` is the pre-built CommandItem. */
  const withTooltip = (key: string, fullTag: string, node: ReactElement) => {
    const description = descriptions.get(fullTag);
    if (!description) return node;
    return (
      <Tooltip key={key}>
        <TooltipTrigger asChild>{node}</TooltipTrigger>
        <TooltipContent side="right" className="max-w-xs">
          {description}
        </TooltipContent>
      </Tooltip>
    );
  };

  return (
    <TooltipProvider delayDuration={200}>
      <div className="p-0">
        <Command shouldFilter={false}>
          <CommandInput
            value={view === null ? rootSearch : valueSearch}
            onValueChange={view === null ? setRootSearch : setValueSearch}
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

            {/* ── Root view: one row per top-level KEY. A key with values shows a
                drill arrow; a key whose bare tag is selectable is clickable to
                select it directly. */}
            {!isLoading && !isError && view === null && (
              <>
                {visibleGroups.length === 0 ? (
                  <CommandEmpty>
                    <span className="text-xs text-muted-foreground">
                      {t("monitoredTables.tagPickerEmpty")}
                    </span>
                  </CommandEmpty>
                ) : (
                  <CommandGroup>
                    {visibleGroups.map((group) => {
                      const hasValues = group.values.length > 0;
                      const item = (
                        <CommandItem
                          key={group.key}
                          value={group.key}
                          // Selecting a drill-only key opens its values; a
                          // selectable bare-tag key emits the bare KEY. When a
                          // key is both, the row text selects the bare tag and
                          // the arrow (below) drills in.
                          onSelect={() => {
                            if (group.hasBareTag) {
                              onSelect(group.key);
                            } else {
                              setValueSearch("");
                              setView(group.key);
                            }
                          }}
                          className="items-center gap-2 text-xs font-mono"
                        >
                          <span className="min-w-0 flex-1 truncate">{group.key}</span>
                          {hasValues && (
                            <button
                              type="button"
                              aria-label={t("monitoredTables.tagPickerDrillIn", { key: group.key })}
                              onClick={(e) => {
                                // Stop cmdk from also firing the row's onSelect.
                                e.preventDefault();
                                e.stopPropagation();
                                setValueSearch("");
                                setView(group.key);
                              }}
                              className="shrink-0 text-muted-foreground hover:text-foreground"
                            >
                              <ArrowRight className="h-3 w-3" />
                            </button>
                          )}
                        </CommandItem>
                      );
                      return withTooltip(group.key, group.key, item);
                    })}
                  </CommandGroup>
                )}
              </>
            )}

            {/* ── Value view: the drilled-into key's `=value` sub-list. Back
                returns to the root. */}
            {!isLoading && !isError && view !== null && (
              <>
                <button
                  type="button"
                  onClick={openRoot}
                  className="flex w-full items-center gap-1.5 px-2 py-1.5 text-[11px] font-medium text-muted-foreground border-b hover:text-foreground"
                >
                  <ArrowLeft className="h-3 w-3 shrink-0" />
                  <span className="truncate font-mono">{view}</span>
                </button>
                {(() => {
                  const values = activeGroup?.values ?? [];
                  const q = valueSearch.trim().toLowerCase();
                  const visibleValues =
                    q === "" ? values : values.filter((v) => v.toLowerCase().includes(q));
                  if (visibleValues.length === 0) {
                    return (
                      <CommandEmpty>
                        <span className="text-xs text-muted-foreground">
                          {t("monitoredTables.tagPickerEmpty")}
                        </span>
                      </CommandEmpty>
                    );
                  }
                  return (
                    <CommandGroup>
                      {visibleValues.map((value) => {
                        const fullTag = `${view}=${value}`;
                        const item = (
                          <CommandItem
                            key={fullTag}
                            value={fullTag}
                            onSelect={() => onSelect(fullTag)}
                            className="text-xs font-mono"
                          >
                            {value}
                          </CommandItem>
                        );
                        return withTooltip(fullTag, fullTag, item);
                      })}
                    </CommandGroup>
                  );
                })()}
              </>
            )}
          </CommandList>
        </Command>
      </div>
    </TooltipProvider>
  );
}
