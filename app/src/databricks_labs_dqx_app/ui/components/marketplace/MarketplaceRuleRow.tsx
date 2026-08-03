import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, Play } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
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
import { RuleTestModal } from "./RuleTestModal";

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
  const [testOpen, setTestOpen] = useState(false);
  const previewRule = checkDictToPreviewRule(rule, checkFunctions, t);
  const tags = [...rule.industries, ...rule.regions];

  return (
    <div className={cn("rounded-md border bg-background transition-colors", selected && "border-primary/50 bg-primary/5")}>
      <div className="flex items-stretch">
        {/* Checkbox aligned to the NAME line (h-9 matches the first row's
            height) rather than floating above the whole cell. */}
        <div className="flex h-9 items-center pl-3">
          <Checkbox
            checked={selected}
            onCheckedChange={onToggleSelect}
            aria-label={t("marketplace.selectRule", { name: rule.name })}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
        {/* Whole button (its own padding) toggles the rule open/closed. */}
        <button
          type="button"
          onClick={onToggleOpen}
          aria-expanded={open}
          className="flex flex-1 items-start gap-2 px-3 py-2 text-left"
        >
          <div className="min-w-0 flex-1 space-y-1">
            <div className="flex h-5 flex-wrap items-center gap-2">
              <span className="text-sm font-medium">{rule.name}</span>
              <TagBadge
                label={rule.dimension}
                color={colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, rule.dimension)}
              />
              <SeverityBadge
                severity={rule.severity}
                color={colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, rule.severity)}
              />
            </div>
            <p className="text-xs text-muted-foreground">{rule.description}</p>
          </div>
          {/* Industry/region tags pinned to the top-right corner. */}
          {tags.length > 0 && (
            <div className="flex max-w-[40%] flex-wrap justify-end gap-1">
              {tags.map((tag) => (
                <TagBadge key={tag} label={formatTagLabel(tag)} />
              ))}
            </div>
          )}
          <ChevronDown
            className={cn(
              "mt-0.5 h-4 w-4 shrink-0 text-muted-foreground transition-transform duration-200",
              open && "rotate-180",
            )}
            aria-hidden
          />
        </button>
      </div>

      {/* Animated open/close via a grid-rows height transition. */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          {/* relative + a right-side gutter (pr-28) so the absolutely-placed
              "Try it out" hugs the bottom-right corner, aligning with the last
              content line (THEN THE ROW) instead of adding its own row and a
              floating gap below. */}
          <div className="relative border-t px-3 pb-3 pl-10 pr-28 pt-3">
            {previewRule ? (
              <RuleLogicBody registryRule={previewRule} />
            ) : (
              <p className="text-xs italic text-muted-foreground">
                {t("monitoredTables.ruleLogicUnavailable")}
              </p>
            )}
            <Button
              type="button"
              variant="secondary"
              size="sm"
              className="absolute bottom-3 right-3 gap-1.5"
              onClick={() => setTestOpen(true)}
            >
              <Play className="h-3.5 w-3.5" aria-hidden />
              {t("marketplace.tryIt")}
            </Button>
          </div>
        </div>
      </div>

      <RuleTestModal
        rule={rule}
        checkFunctions={checkFunctions}
        open={testOpen}
        onOpenChange={setTestOpen}
      />
    </div>
  );
}
