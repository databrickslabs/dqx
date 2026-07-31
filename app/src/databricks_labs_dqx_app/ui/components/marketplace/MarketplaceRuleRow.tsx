import { useTranslation } from "react-i18next";
import { ChevronDown } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";
import type { CheckFunctionDef, MarketplaceRuleOut } from "@/lib/api";
import type { LabelColorDefinition } from "@/components/RegistryRuleBadges";
import {
  TagBadge,
  SeverityBadge,
  colorFor,
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
} from "@/components/RegistryRuleBadges";
import { RuleLogicBody } from "@/components/apply-rules/RuleConfigCard";
import { checkDictToPreviewRule, formatTagLabel } from "@/lib/marketplace-selection";

export function MarketplaceRuleRow({
  rule,
  selected,
  onToggleSelect,
  open,
  onToggleOpen,
  checkFunctions,
  labelDefinitions,
}: {
  rule: MarketplaceRuleOut;
  selected: boolean;
  onToggleSelect: () => void;
  open: boolean;
  onToggleOpen: () => void;
  checkFunctions: CheckFunctionDef[];
  labelDefinitions: LabelColorDefinition[];
}) {
  const { t } = useTranslation();
  const previewRule = checkDictToPreviewRule(rule, checkFunctions, t);

  return (
    <div className={cn("rounded-md border transition-colors", selected && "border-primary/50 bg-primary/5")}>
      <div className="flex items-start gap-3 px-3 py-2">
        <Checkbox
          checked={selected}
          onCheckedChange={onToggleSelect}
          aria-label={t("marketplace.selectRule", { name: rule.name })}
          onClick={(e) => e.stopPropagation()}
          className="mt-0.5"
        />
        {/* Single toggle: the button owns open/close. The disclosure body
            below is a plain animated region (no second toggle), so one click
            always opens exactly what was clicked — no close-one-layer bug. */}
        <button
          type="button"
          onClick={onToggleOpen}
          aria-expanded={open}
          className="flex flex-1 items-start gap-2 text-left"
        >
          <div className="flex-1 space-y-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium">{rule.name}</span>
              <TagBadge
                label={rule.dimension}
                color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, rule.dimension)}
              />
              <SeverityBadge
                severity={rule.severity}
                color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, rule.severity)}
              />
              {rule.industries.map((i) => (
                <TagBadge key={`i-${i}`} label={formatTagLabel(i)} />
              ))}
              {rule.regions.map((r) => (
                <TagBadge key={`r-${r}`} label={formatTagLabel(r)} />
              ))}
            </div>
            <p className="text-xs text-muted-foreground">{rule.description}</p>
          </div>
          <ChevronDown
            className={cn(
              "mt-0.5 h-4 w-4 shrink-0 text-muted-foreground transition-transform duration-200",
              open && "rotate-180",
            )}
            aria-hidden
          />
        </button>
      </div>

      {/* Animated open/close via a grid-rows height transition. The open
          state is owned by the parent pack accordion (one rule open at a
          time), so switching rules animates the old one closed and the new
          one open in a single click. */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          <div className="border-t px-3 pb-3 pl-10 pt-3">
            {previewRule ? (
              <RuleLogicBody registryRule={previewRule} />
            ) : (
              <p className="text-xs italic text-muted-foreground">
                {t("monitoredTables.ruleLogicUnavailable")}
              </p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
