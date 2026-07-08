import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Badge } from "@/components/ui/badge";
import { Check, ChevronDown, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { FAMILY_LABEL, type Family } from "@/lib/lowcodeOperators";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

type Props = {
  value: string;
  onChange: (v: string) => void;
  declaredColumns: LowcodeColumnRef[];
  disabled?: boolean;
};

// Structured Group-By column picker, ported from dqlake's GroupByField.
//
// Replaces the earlier free-text `GroupBySqlField`: a raw SQL field let a
// steward type an expression such as `COALESCE({{country}}, 'XX')`, whose
// embedded comma corrupted the compiled `merge_columns` AND — because a
// grouping expression is not a real column on the monitored table — could
// never satisfy DQX's row-level merge-back contract (`merge_columns` must
// exist on the input DataFrame). Restricting grouping to declared column refs
// makes both failure modes unrepresentable.
//
// The persisted `value` is a comma-joined list of `{{slot}}` tokens so the
// materializer binds each to a real column at apply time.
function refToken(name: string): string {
  return name.includes(".") ? name : `{{${name}}}`;
}

function buildValue(names: string[]): string {
  return names.map(refToken).join(", ");
}

function parseRefs(value: string, declared: LowcodeColumnRef[]): string[] {
  const declaredSet = new Set(declared.map((c) => c.name));
  const out: string[] = [];
  for (const raw of value.split(",")) {
    const p = raw.trim();
    if (!p) continue;
    const m = /^\{\{(.+?)\}\}$/.exec(p);
    const ref = m ? m[1] : p;
    if (declaredSet.has(ref)) out.push(ref);
  }
  return out;
}

export function GroupByField({ value, onChange, declaredColumns, disabled }: Props) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);

  const selected = useMemo(() => parseRefs(value, declaredColumns), [value, declaredColumns]);

  // If a column is removed from Columns Used (or its join goes away), prune the
  // stored value so save-time compilation never references a stale column.
  useEffect(() => {
    const declaredSet = new Set(declaredColumns.map((c) => c.name));
    const refs = parseRefs(value, declaredColumns);
    const stillValid = refs.filter((r) => declaredSet.has(r));
    if (stillValid.length !== refs.length) {
      onChange(buildValue(stillValid));
    }
  }, [declaredColumns, value, onChange]);

  const selectedSet = new Set(selected);
  const hasColumns = declaredColumns.length > 0;

  const toggle = (name: string) => {
    const next = selectedSet.has(name) ? selected.filter((n) => n !== name) : [...selected, name];
    onChange(buildValue(next));
  };

  return (
    <div className="flex flex-col gap-2">
      <Label className="text-xs">{t("rulesRegistry.lowcodeGroupByLabel")}</Label>
      <p className="text-[10px] text-muted-foreground">{t("rulesRegistry.lowcodeGroupByHelp")}</p>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button
            type="button"
            variant="outline"
            role="combobox"
            aria-expanded={open}
            className="w-full justify-between font-normal h-auto min-h-8 py-1.5"
            disabled={disabled || !hasColumns}
          >
            {selected.length === 0 ? (
              <span className="text-muted-foreground text-xs">
                {hasColumns
                  ? t("rulesRegistry.lowcodeGroupByPlaceholder")
                  : t("rulesRegistry.lowcodeGroupByNoColumns")}
              </span>
            ) : (
              <span className="flex flex-wrap gap-1.5 max-w-full">
                {selected.map((n) => (
                  <Badge key={n} variant="secondary" className="font-mono text-xs">
                    {n}
                  </Badge>
                ))}
              </span>
            )}
            <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          </Button>
        </PopoverTrigger>
        <PopoverContent className="min-w-[20rem] w-[--radix-popover-trigger-width] p-0" align="start">
          <Command>
            <CommandInput placeholder={t("rulesRegistry.lowcodeGroupBySearch")} />
            <CommandList>
              <CommandEmpty>{t("rulesRegistry.lowcodeGroupByEmpty")}</CommandEmpty>
              {selected.length > 0 && (
                <CommandGroup>
                  <CommandItem onSelect={() => onChange("")} className="text-muted-foreground italic">
                    <X className="mr-2 h-3.5 w-3.5" />
                    {t("rulesRegistry.lowcodeGroupByClear")}
                  </CommandItem>
                </CommandGroup>
              )}
              <CommandGroup>
                {declaredColumns.map((col) => {
                  const isOn = selectedSet.has(col.name);
                  return (
                    <CommandItem
                      key={col.name}
                      value={col.name}
                      onSelect={() => toggle(col.name)}
                      className="flex items-center gap-2"
                    >
                      <Check className={cn("h-4 w-4", isOn ? "opacity-100" : "opacity-0")} />
                      <span className="font-mono text-sm flex-1 truncate" title={col.name}>
                        {col.name}
                      </span>
                      <Badge variant="outline" className="text-[10px]">
                        {FAMILY_LABEL[col.family as Family] ?? col.family}
                      </Badge>
                    </CommandItem>
                  );
                })}
              </CommandGroup>
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  );
}
