import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { ChevronDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import {
  AGGREGATES,
  AGGREGATES_TAKING_PARAM,
  aggregateAcceptsFamily,
  AGGREGATE_INPUT_FAMILIES,
  FAMILY_LABEL,
  type Family,
} from "@/lib/lowcodeOperators";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";
import { cn } from "@/lib/utils";

/** ALL-CAPS framing-word style for cmdk group headings — matches the merged
 * condition selector's section headers (IF / THEN THE ROW style). */
const AGG_GROUP_HEADING_CLASS =
  "[&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-wider [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:text-[10px]";

type Props = {
  aggregate: string;
  column_ref: string;
  aggregate_param?: number | null;
  declaredColumns: LowcodeColumnRef[];
  onChange: (next: { aggregate: string; column_ref: string; aggregate_param?: number | null }) => void;
};

// Friendly labels for the dropdown. Ordering follows AGGREGATES.
const AGG_LABEL: Record<string, string> = {
  count: "count",
  count_distinct: "count distinct",
  null_rate: "null rate",
  approx_count_distinct: "approx count distinct",
  sum: "sum",
  avg: "avg",
  min: "min",
  max: "max",
  stddev: "stddev (population)",
  stddev_samp: "stddev (sample)",
  variance: "variance (population)",
  var_samp: "variance (sample)",
  median: "median",
  percentile: "percentile",
  percentile_approx: "percentile (approx)",
  bool_and: "all true (bool_and)",
  bool_or: "any true (bool_or)",
  any_value: "any value",
  mode: "mode",
};

function familyOf(name: string, declared: LowcodeColumnRef[]): Family | null {
  const d = declared.find((c) => c.name === name);
  return d ? d.family : null;
}

const UNIVERSAL_AGGREGATES = AGGREGATES.filter((a) => AGGREGATE_INPUT_FAMILIES[a] === "ANY");

// Ported from dqlake's AggregatedFieldArea.
export function AggregatedFieldArea({ aggregate, column_ref, aggregate_param, declaredColumns, onChange }: Props) {
  const { t } = useTranslation();
  const [aggOpen, setAggOpen] = useState(false);
  const [aggQuery, setAggQuery] = useState("");
  const colFamily = familyOf(column_ref, declaredColumns);

  const familySpecific = AGGREGATES.filter((a) => {
    if (UNIVERSAL_AGGREGATES.includes(a)) return false;
    if (!colFamily) return true;
    return aggregateAcceptsFamily(a, colFamily);
  });

  const aggSpec = aggregate ? AGGREGATE_INPUT_FAMILIES[aggregate] : undefined;
  const allowedColumns = declaredColumns.filter((c) =>
    !aggregate ? true : aggSpec === "ANY" ? true : Array.isArray(aggSpec) && aggSpec.includes(c.family),
  );

  const needsParam = AGGREGATES_TAKING_PARAM.has(aggregate);
  const familyGroupLabel = colFamily ? FAMILY_LABEL[colFamily] : t("rulesRegistry.lowcodeTypeSpecific");

  // Grouped + query-filtered aggregate options for the searchable picker,
  // mirroring the merged condition selector's operators view.
  const aggGroups: { heading: string; aggs: string[] }[] = [];
  const q = aggQuery.trim().toLowerCase();
  const matches = (a: string) => q === "" || (AGG_LABEL[a] ?? a).toLowerCase().includes(q) || a.toLowerCase().includes(q);
  const familyMatches = familySpecific.filter(matches);
  const universalMatches = UNIVERSAL_AGGREGATES.filter(matches);
  if (familyMatches.length > 0) aggGroups.push({ heading: familyGroupLabel, aggs: familyMatches });
  if (universalMatches.length > 0)
    aggGroups.push({ heading: t("rulesRegistry.lowcodeUniversalOperators"), aggs: universalMatches });

  return (
    <div className={needsParam ? "grid grid-cols-[1fr_1fr_64px] gap-1" : "grid grid-cols-2 gap-1"}>
      {/* Aggregate-function picker in the merged-dropdown style: a searchable
          Command popover with ALL-CAPS section headers + monospace items,
          matching the condition operator selector. */}
      <Popover
        open={aggOpen}
        onOpenChange={(o) => {
          setAggOpen(o);
          if (!o) setAggQuery("");
        }}
      >
        <PopoverTrigger asChild>
          <button
            type="button"
            data-slot="select-trigger"
            data-size="sm"
            className="border-input dark:bg-input/30 dark:hover:bg-input/50 flex h-8 w-full items-center justify-between gap-2 rounded-md border bg-transparent px-3 py-1 font-mono text-xs whitespace-nowrap shadow-xs outline-none"
          >
            <span className={cn("truncate", aggregate ? "text-foreground" : "text-muted-foreground")}>
              {aggregate ? (AGG_LABEL[aggregate] ?? aggregate) : t("rulesRegistry.lowcodeAggregatePlaceholder")}
            </span>
            <ChevronDown className="size-4 opacity-50 shrink-0" />
          </button>
        </PopoverTrigger>
        <PopoverContent className="p-0 w-auto min-w-56 max-w-[24rem]" align="start">
          <Command shouldFilter={false}>
            <CommandInput
              placeholder={t("rulesRegistry.lowcodeAggregatePlaceholder")}
              value={aggQuery}
              onValueChange={setAggQuery}
              className="h-8 text-xs"
            />
            <CommandList className="max-h-72">
              {aggGroups.length === 0 && (
                <CommandEmpty>
                  <span className="text-xs text-muted-foreground">{t("rulesRegistry.noMatches")}</span>
                </CommandEmpty>
              )}
              {aggGroups.map(({ heading, aggs }) => (
                <CommandGroup key={heading} heading={heading} className={AGG_GROUP_HEADING_CLASS}>
                  {aggs.map((a) => (
                    <CommandItem
                      key={a}
                      value={a}
                      onSelect={() => {
                        onChange({
                          aggregate: a,
                          column_ref,
                          aggregate_param: AGGREGATES_TAKING_PARAM.has(a) ? (aggregate_param ?? 0.95) : null,
                        });
                        setAggOpen(false);
                      }}
                      className="text-xs font-mono"
                    >
                      {AGG_LABEL[a] ?? a}
                    </CommandItem>
                  ))}
                </CommandGroup>
              ))}
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
      <Select value={column_ref || ""} onValueChange={(v) => onChange({ aggregate, column_ref: v, aggregate_param })}>
        <SelectTrigger className="h-8 w-full font-mono text-xs">
          <SelectValue placeholder={t("rulesRegistry.lowcodeColumnPlaceholder")} />
        </SelectTrigger>
        <SelectContent>
          {allowedColumns.map((c) => (
            <SelectItem key={c.name} value={c.name} className="font-mono text-xs">
              {c.name.includes(".") ? c.name : `{{${c.name}}}`}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {needsParam && (
        <Input
          type="number"
          step="0.01"
          min={0}
          max={1}
          value={aggregate_param ?? ""}
          placeholder="0.95"
          onChange={(e) => {
            const v = e.target.value;
            onChange({ aggregate, column_ref, aggregate_param: v === "" ? null : Number(v) });
          }}
          className="h-8 font-mono text-xs"
          title={t("rulesRegistry.lowcodeQuantileTitle")}
        />
      )}
    </div>
  );
}
