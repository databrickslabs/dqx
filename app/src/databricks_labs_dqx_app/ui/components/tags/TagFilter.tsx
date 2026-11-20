// TagFilter — the tag filter bubble shared by the Rules, Tables, Collections
// and Results pages.
//
// Same layout as the rule editor's TagPicker (search box, one row per tag
// key, drilling into a key's values with a back header), with checkboxes for
// multi-select. Clicking a key row opens its values; ticking its checkbox
// selects any value of it. A row with nothing below it (a value, or a
// key-only tag) toggles on click. Keys are grouped under a heading per source
// (rule, column, table, schema, catalog), and the trigger names the source of
// the pick, so it is always clear where a tag comes from. Selection
// semantics live in ./tag-filter.ts.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ArrowLeft, ArrowRight, ChevronDown } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { TruncatedLabel } from "@/components/data-table/TruncatedLabel";
import { FILTER_TRIGGER_PICKED_CLASS } from "@/components/data-table/filter-bar";
import { FACET_TRIGGER_CLASS } from "@/components/results/ResultsFacetFilters";
import { cn } from "@/lib/utils";
import {
  EMPTY_TAG_SELECTION,
  filterTagGroups,
  groupTagOptions,
  hasValuesBelow,
  keyState,
  TAG_SOURCES,
  tagTokenLabel,
  toggleKey,
  toggleValue,
  valueSelected,
  type TagKeyGroup,
  type TagLabels,
  type TagOption,
  type TagSelection,
  type TagSource,
} from "./tag-filter";

/** Display names per source ("Table tag"), for triggers, chips and chart labels. */
export function useTagLabels(): TagLabels {
  const { t } = useTranslation();
  return {
    untagged: t("tagFilter.untagged"),
    source: {
      rule: t("tagFilter.sourceNameRule"),
      column: t("tagFilter.sourceNameColumn"),
      table: t("tagFilter.sourceNameTable"),
      schema: t("tagFilter.sourceNameSchema"),
      catalog: t("tagFilter.sourceNameCatalog"),
    },
  };
}

const SOURCE_HEADING_KEY: Record<TagSource, string> = {
  catalog: "tagFilter.sourceCatalog",
  schema: "tagFilter.sourceSchema",
  table: "tagFilter.sourceTable",
  column: "tagFilter.sourceColumn",
  rule: "tagFilter.sourceRule",
};

export interface TagFilterProps {
  options: readonly TagOption[];
  selected: TagSelection;
  onChange: (next: TagSelection) => void;
  className?: string;
}

export function TagFilter({ options, selected, onChange, className }: TagFilterProps) {
  const { t } = useTranslation();
  const labels = useTagLabels();
  const [open, setOpen] = useState(false);
  // null at the root; a tag id once drilled into that key's values.
  const [view, setView] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  const groups = useMemo(() => groupTagOptions(options), [options]);
  const active = view === null ? null : (groups.find((g) => g.id === view) ?? null);

  const valueLabel = (value: string | null) =>
    value === null ? t("tagFilter.untagged") : value === "" ? t("tagFilter.noValue") : value;

  // Names the source of the pick ("Table tag · layer: gold"), never a bare key.
  const triggerLabel = (() => {
    const ids = [...selected.keys()];
    if (ids.length === 0) return t("tagFilter.allTags");
    if (ids.length > 1) return t("tagFilter.selectedCount", { count: ids.length });
    const id = ids[0];
    const values = [...(selected.get(id) ?? [])];
    if (values.length === 0) return tagTokenLabel(id, labels);
    if (values.length === 1) {
      return tagTokenLabel(values[0] === null ? `!${id}` : `${id}=${values[0]}`, labels);
    }
    return t("tagFilter.keyValuesCount", { key: tagTokenLabel(id, labels), count: values.length });
  })();

  const openRoot = () => {
    setView(null);
    setSearch("");
  };
  const drillInto = (id: string) => {
    setView(id);
    setSearch("");
  };
  const onOpenChange = (next: boolean) => {
    setOpen(next);
    if (!next) openRoot();
  };

  const keyRow = (group: TagKeyGroup) => {
    const state = keyState(selected, group.id);
    const narrowed = state === "some" ? (selected.get(group.id)?.size ?? 0) : 0;
    const drillable = hasValuesBelow(group);
    return (
      <CommandItem
        key={group.id}
        value={group.id}
        // The row opens the key's values; a key with none below it toggles.
        onSelect={() => (drillable ? drillInto(group.id) : onChange(toggleKey(selected, group.id)))}
        aria-label={drillable ? t("tagFilter.showValues", { key: group.key }) : undefined}
        className={cn("items-center gap-2 text-xs", state !== "none" && "bg-primary/10")}
      >
        {/* The checkbox always ticks the whole key; stop cmdk from also drilling in. */}
        <span
          className="shrink-0"
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            onChange(toggleKey(selected, group.id));
          }}
        >
          <Checkbox
            checked={state === "all" ? true : state === "some" ? "indeterminate" : false}
            aria-label={group.key}
            className="pointer-events-none"
          />
        </span>
        <span className="min-w-0 flex-1 truncate font-mono" title={group.key}>
          {group.key}
        </span>
        {narrowed > 0 && <span className="shrink-0 text-[10px] text-muted-foreground">{narrowed}</span>}
        {drillable && <ArrowRight className="h-3 w-3 shrink-0 text-muted-foreground" aria-hidden />}
      </CommandItem>
    );
  };

  const rootView = () => {
    const visible = filterTagGroups(groups, search);
    if (visible.length === 0) {
      return (
        <CommandEmpty>
          <span className="text-xs text-muted-foreground">{t("tagFilter.noTags")}</span>
        </CommandEmpty>
      );
    }
    return TAG_SOURCES.filter((source) => visible.some((g) => g.source === source)).map((source) => (
      <CommandGroup key={source} heading={t(SOURCE_HEADING_KEY[source])}>
        {visible.filter((g) => g.source === source).map(keyRow)}
      </CommandGroup>
    ));
  };

  const valueView = (group: TagKeyGroup) => {
    const q = search.trim().toLowerCase();
    const values = group.values.filter((v) => !q || valueLabel(v).toLowerCase().includes(q));
    return (
      <>
        <button
          type="button"
          onClick={openRoot}
          className="flex w-full items-center gap-1.5 border-b px-2 py-1.5 text-[11px] font-medium text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="h-3 w-3 shrink-0" />
          <span className="truncate font-mono">{group.key}</span>
        </button>
        {values.length === 0 ? (
          <CommandEmpty>
            <span className="text-xs text-muted-foreground">{t("tagFilter.noTags")}</span>
          </CommandEmpty>
        ) : (
          <CommandGroup>
            {values.map((value) => {
              const checked = valueSelected(selected, group.id, value);
              return (
                <CommandItem
                  key={value ?? "\u0000"}
                  value={`${group.id}=${value ?? ""}`}
                  onSelect={() => onChange(toggleValue(selected, group, value))}
                  className={cn("text-xs", checked && "bg-primary/10")}
                >
                  <Checkbox checked={checked} className="shrink-0 pointer-events-none" />
                  <span className={cn("min-w-0 flex-1 truncate", value === null ? "italic" : "font-mono")}>
                    {valueLabel(value)}
                  </span>
                </CommandItem>
              );
            })}
          </CommandGroup>
        )}
      </>
    );
  };

  return (
    <Popover open={open} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        <button
          type="button"
          role="combobox"
          aria-expanded={open}
          aria-label={t("tagFilter.allTags")}
          className={cn(
            FACET_TRIGGER_CLASS,
            selected.size > 0 && cn(FILTER_TRIGGER_PICKED_CLASS, "border-ring/60 text-foreground"),
            className,
          )}
        >
          <TruncatedLabel text={triggerLabel} />
          <ChevronDown className="size-4 shrink-0 opacity-50" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-64 min-w-(--radix-popover-trigger-width) p-0">
        <Command shouldFilter={false}>
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={t("tagFilter.searchTags")}
            className="h-8 text-xs"
          />
          <CommandList className="max-h-64">{active ? valueView(active) : rootView()}</CommandList>
        </Command>
        {selected.size > 0 && (
          <div className="flex justify-end border-t p-1">
            <Button variant="ghost" size="sm" className="h-6 text-[11px]" onClick={() => onChange(EMPTY_TAG_SELECTION)}>
              {t("tagFilter.clear")}
            </Button>
          </div>
        )}
      </PopoverContent>
    </Popover>
  );
}
