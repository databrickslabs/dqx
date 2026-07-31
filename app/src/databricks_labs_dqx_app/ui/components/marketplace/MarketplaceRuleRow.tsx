import { useTranslation } from "react-i18next";
import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";
import type { CheckFunctionDef, MarketplaceRuleOut } from "@/lib/api";
import { TagBadge, SeverityBadge } from "@/components/RegistryRuleBadges";
import { RuleLogicDisclosure } from "@/components/apply-rules/RuleConfigCard";
import { checkDictToPreviewRule } from "@/lib/marketplace-selection";

export function MarketplaceRuleRow({
  rule,
  selected,
  onToggleSelect,
  open,
  onToggleOpen,
  checkFunctions,
}: {
  rule: MarketplaceRuleOut;
  selected: boolean;
  onToggleSelect: () => void;
  open: boolean;
  onToggleOpen: () => void;
  checkFunctions: CheckFunctionDef[];
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
        <button type="button" onClick={onToggleOpen} className="flex-1 text-left space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium">{rule.name}</span>
            <TagBadge label={rule.dimension} />
            <SeverityBadge severity={rule.severity} />
            {rule.industries.map((i) => (
              <TagBadge key={`i-${i}`} label={i} />
            ))}
            {rule.regions.map((r) => (
              <TagBadge key={`r-${r}`} label={r} />
            ))}
          </div>
          <p className="text-xs text-muted-foreground">{rule.description}</p>
        </button>
      </div>
      {open && (
        <div className="px-3 pb-3">
          <RuleLogicDisclosure open onToggle={onToggleOpen} registryRule={previewRule} />
        </div>
      )}
    </div>
  );
}
