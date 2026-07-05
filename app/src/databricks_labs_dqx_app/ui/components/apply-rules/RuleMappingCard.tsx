// RuleMappingCard — the AddRulesDialog "map columns" step, restyled to
// reuse the same card chrome as the by-rule lens's RuleConfigCard (dimension
// badge, rule-logic disclosure) and the same per-slot grid layout as
// MappingChips (`{{slot}}` + family badge -> arrow -> value), instead of a
// bespoke stacked form. The rule isn't applied yet at this point, so there's
// no AppliedRuleOut/MappingChips to render read-only chips from — this
// component fills the same visual slot with live column pickers instead.
// Mirrors dqlake's AddRulesDialog reusing bindings/RuleConfigCard's layout
// for the mapping step.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { ColumnOut, RegistryRuleOut, RuleSlot } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import { RuleLogicDisclosure } from "./RuleConfigCard";
import { MultiColumnPicker, SingleColumnPicker } from "./ColumnPicker";
import { RESERVED_DIMENSION_KEY, TagBadge, colorFor, getTag, RESERVED_NAME_KEY } from "./shared";

function FamilyBadge({ family }: { family: string }) {
  if (!family) return null;
  return (
    <span className="inline-block rounded bg-muted/60 border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground font-medium uppercase tracking-wide shrink-0">
      {family}
    </span>
  );
}

interface RuleMappingCardProps {
  rule: RegistryRuleOut;
  columns: ColumnOut[];
  mapping: Record<string, string | string[]>;
  onChange: (slotName: string, value: string | string[]) => void;
  labelDefinitions: LabelDefinition[];
  /** Column names already mapped to this rule (e.g. via other mapping
   *  groups) — excluded from every slot's candidate list so the "+ Apply to
   *  another column" flow can't re-pick a column the rule already covers.
   *  See `getUsedColumnsForRule`. */
  excludeColumns?: string[];
}

/** Editable analog of `RuleConfigCard` + `MappingChips`, used while a rule is
 *  being mapped (before it's applied) in `AddRulesDialog`'s map step. */
export function RuleMappingCard({
  rule,
  columns,
  mapping,
  onChange,
  labelDefinitions,
  excludeColumns,
}: RuleMappingCardProps) {
  const { t } = useTranslation();
  const [logicOpen, setLogicOpen] = useState(false);
  const dimension = getTag(rule, RESERVED_DIMENSION_KEY);
  const slots: RuleSlot[] = rule.definition.slots ?? [];

  return (
    <div className="rounded-lg border bg-card overflow-hidden">
      <div className="flex items-center gap-3 px-4 py-3">
        <div className="min-w-0 flex-1">
          <p className="font-semibold text-sm leading-snug">{getTag(rule, RESERVED_NAME_KEY) || rule.rule_id}</p>
          <div className="flex flex-wrap gap-1 mt-1">
            <TagBadge label={dimension} color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dimension)} />
          </div>
        </div>
      </div>

      <div className="border-t px-4 py-4 space-y-3">
        <RuleLogicDisclosure open={logicOpen} onToggle={() => setLogicOpen((p) => !p)} registryRule={rule} />

        {slots.length === 0 ? (
          <p className="text-xs text-muted-foreground italic">{t("monitoredTables.mapColumnsTooltip")}</p>
        ) : (
          <div className="space-y-2">
            {slots.map((slot) => {
              const many = slot.cardinality === "many";
              return (
                <div key={slot.name} className="grid grid-cols-[160px_24px_1fr] items-center gap-3">
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="font-mono text-xs truncate">{`{{${slot.name}}}`}</span>
                    <FamilyBadge family={slot.family} />
                  </div>
                  <span className="text-muted-foreground text-xs justify-self-center self-center" aria-hidden>
                    &rarr;
                  </span>
                  {many ? (
                    <MultiColumnPicker
                      slot={slot}
                      columns={columns}
                      value={(mapping[slot.name] as string[] | undefined) ?? []}
                      onChange={(next) => onChange(slot.name, next)}
                      excludeColumns={excludeColumns}
                    />
                  ) : (
                    <SingleColumnPicker
                      slot={slot}
                      columns={columns}
                      value={mapping[slot.name] as string | undefined}
                      onChange={(v) => onChange(slot.name, v)}
                      excludeColumns={excludeColumns}
                    />
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
