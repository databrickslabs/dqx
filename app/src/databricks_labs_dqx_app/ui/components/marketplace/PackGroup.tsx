import { useTranslation } from "react-i18next";
import * as Icons from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { ChevronDown } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";
import type { CheckFunctionDef, MarketplacePackOut } from "@/lib/api";
import { packSelectionState } from "@/lib/marketplace-selection";
import { MarketplaceRuleRow } from "./MarketplaceRuleRow";

export function PackGroup({
  pack,
  selected,
  onToggleRule,
  onTogglePack,
  openRuleKey,
  onOpenRule,
  checkFunctions,
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
    <div className="rounded-lg border">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3">
        <Checkbox
          checked={selState === "all" ? true : selState === "some" ? "indeterminate" : false}
          onCheckedChange={handlePackCheckbox}
          aria-label={t("marketplace.selectPack", { title: pack.title })}
          onClick={(e) => e.stopPropagation()}
        />
        <Icon className="h-4 w-4 text-muted-foreground shrink-0" aria-hidden />
        <button
          type="button"
          onClick={onToggleExpanded}
          className="flex flex-1 items-center gap-2 text-left"
        >
          <span className="text-sm font-semibold">{pack.title}</span>
          <span className="text-xs text-muted-foreground ml-auto shrink-0">
            {t("marketplace.packSelectedCount", {
              selected: selectedCount,
              total: pack.rules.length,
            })}
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
            <p className="text-xs text-muted-foreground">{pack.description}</p>
            {pack.rules.map((rule) => (
              <MarketplaceRuleRow
                key={rule.rule_key}
                rule={rule}
                selected={selected.has(rule.rule_key)}
                onToggleSelect={() => onToggleRule(rule.rule_key)}
                open={openRuleKey === rule.rule_key}
                onToggleOpen={() => handleRuleToggleOpen(rule.rule_key)}
                checkFunctions={checkFunctions}
              />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
