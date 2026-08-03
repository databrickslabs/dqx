import { useTranslation } from "react-i18next";
import * as Icons from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { ChevronDown } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";
import type { CheckFunctionDef, MarketplacePackOut } from "@/lib/api";
import type { LabelColorDefinition } from "@/components/RegistryRuleBadges";
import { packSelectionState } from "@/lib/marketplace-selection";
import { MarketplaceRuleRow } from "./MarketplaceRuleRow";
import { AnimatedCount } from "./AnimatedCount";

export function PackGroup({
  pack,
  selected,
  onToggleRule,
  onTogglePack,
  openRuleKey,
  onOpenRule,
  checkFunctions,
  labelDefinitions,
  expanded,
  onToggleExpanded,
}: {
  pack: MarketplacePackOut;
  selected: Set<string>;
  onToggleRule: (key: string) => void;
  onTogglePack: (packRuleKeys: string[]) => void;
  openRuleKey: string | null;
  onOpenRule: (key: string | null) => void;
  checkFunctions: CheckFunctionDef[];
  labelDefinitions: LabelColorDefinition[];
  expanded: boolean;
  onToggleExpanded: () => void;
}) {
  const { t } = useTranslation();

  const packRuleKeys = pack.rules.map((r) => r.rule_key);
  const selState = packSelectionState(packRuleKeys, selected);
  const selectedCount = packRuleKeys.filter((k) => selected.has(k)).length;

  const Icon = ((Icons as Record<string, unknown>)[pack.icon] as LucideIcon | undefined) ?? Icons.Package;

  function handlePackCheckbox() {
    onTogglePack(packRuleKeys);
  }

  function handleRuleToggleOpen(key: string) {
    onOpenRule(openRuleKey === key ? null : key);
  }

  return (
    <div className="rounded-lg border bg-muted/50">
      {/* Header — checkbox cell + a full-height toggle button. The hover tint
          lives on the WHOLE row wrapper (not just the button) so the checkbox
          cell and the toggle tint together as one surface rather than
          two-tone. Padding stays on the button so the entire header area
          toggles the pack, not just its vertical centre. Rounding matches the
          card so the tint doesn't spill past the corners. */}
      <div
        className={cn(
          "flex items-stretch rounded-t-lg transition-colors hover:bg-muted",
          !expanded && "rounded-b-lg",
        )}
      >
        <div className="flex items-center pl-4">
          <Checkbox
            checked={selState === "all" ? true : selState === "some" ? "indeterminate" : false}
            onCheckedChange={handlePackCheckbox}
            aria-label={t("marketplace.selectPack", { title: pack.title })}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
        <button
          type="button"
          onClick={onToggleExpanded}
          aria-expanded={expanded}
          className="flex flex-1 items-center gap-3 px-3 py-3 text-left"
        >
          <Icon className="h-4 w-4 text-muted-foreground shrink-0" aria-hidden />
          <span className="text-sm font-semibold">{pack.title}</span>
          <span className="ml-auto shrink-0 text-xs text-muted-foreground">
            <AnimatedCount value={selectedCount} /> / <AnimatedCount value={pack.rules.length} />{" "}
            {t("marketplace.selectedLabel")}
          </span>
          <ChevronDown
            className={cn(
              "h-4 w-4 text-muted-foreground transition-transform shrink-0",
              expanded && "rotate-180",
            )}
            aria-hidden
          />
        </button>
      </div>

      {/* Collapsible rule list */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          expanded ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          <div className="px-4 pb-4 space-y-2 border-t pt-3">
            {pack.rules.map((rule) => (
              <MarketplaceRuleRow
                key={rule.rule_key}
                rule={rule}
                selected={selected.has(rule.rule_key)}
                onToggleSelect={() => onToggleRule(rule.rule_key)}
                open={openRuleKey === rule.rule_key}
                onToggleOpen={() => handleRuleToggleOpen(rule.rule_key)}
                checkFunctions={checkFunctions}
                labelDefinitions={labelDefinitions}
              />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
