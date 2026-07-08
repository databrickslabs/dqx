import { useTranslation } from "react-i18next";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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

  return (
    <div className={needsParam ? "grid grid-cols-[1fr_1fr_64px] gap-1" : "grid grid-cols-2 gap-1"}>
      <Select
        value={aggregate || ""}
        onValueChange={(v) =>
          onChange({
            aggregate: v,
            column_ref,
            aggregate_param: AGGREGATES_TAKING_PARAM.has(v) ? (aggregate_param ?? 0.95) : null,
          })
        }
      >
        <SelectTrigger className="h-8 font-mono text-xs">
          <SelectValue placeholder={t("rulesRegistry.lowcodeAggregatePlaceholder")} />
        </SelectTrigger>
        <SelectContent>
          {familySpecific.length > 0 && (
            <SelectGroup>
              <SelectLabel className="text-[10px] uppercase tracking-wider text-muted-foreground">
                {familyGroupLabel}
              </SelectLabel>
              {familySpecific.map((a) => (
                <SelectItem key={a} value={a}>
                  {AGG_LABEL[a] ?? a}
                </SelectItem>
              ))}
            </SelectGroup>
          )}
          {familySpecific.length > 0 && UNIVERSAL_AGGREGATES.length > 0 && <SelectSeparator />}
          <SelectGroup>
            <SelectLabel className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {t("rulesRegistry.lowcodeUniversalOperators")}
            </SelectLabel>
            {UNIVERSAL_AGGREGATES.map((a) => (
              <SelectItem key={a} value={a}>
                {AGG_LABEL[a] ?? a}
              </SelectItem>
            ))}
          </SelectGroup>
        </SelectContent>
      </Select>
      <Select value={column_ref || ""} onValueChange={(v) => onChange({ aggregate, column_ref: v, aggregate_param })}>
        <SelectTrigger className="h-8 font-mono text-xs">
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
