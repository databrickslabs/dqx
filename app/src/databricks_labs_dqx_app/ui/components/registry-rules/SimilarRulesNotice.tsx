import { useTranslation } from "react-i18next";
import { Copy } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import type { CreateRegistryRuleIn } from "@/lib/api";
import { parameterSummary, type SimilarRuleChoice } from "@/lib/similar-rules";

/** Lets the importer reuse existing rules that run the same check with different settings. */
export function SimilarRulesNotice({
  choices,
  inputs,
  reuseRuleIds,
  onToggle,
  disabled = false,
}: {
  choices: SimilarRuleChoice[];
  inputs: CreateRegistryRuleIn[];
  reuseRuleIds: ReadonlySet<string>;
  onToggle: (ruleId: string, reuse: boolean) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  if (choices.length === 0) return null;
  const defaults = t("similarRules.defaultSettings");

  return (
    <div className="space-y-2 rounded-lg bg-amber-50 p-3 text-sm text-amber-800 dark:bg-amber-950/30 dark:text-amber-300">
      <div className="flex items-start gap-2">
        <Copy className="mt-0.5 h-4 w-4 shrink-0" />
        <div className="space-y-0.5">
          <p className="text-xs font-medium">
            {t("similarRules.title", {
              count: choices.reduce((n, c) => n + c.inputIndexes.length, 0),
            })}
          </p>
          <p className="text-xs opacity-80">{t("similarRules.description")}</p>
        </div>
      </div>
      <ul className="space-y-2 pl-6">
        {choices.map(({ rule, inputIndexes }) => {
          const name = String(rule.user_metadata?.["name"] ?? rule.rule_id);
          const reuse = reuseRuleIds.has(rule.rule_id);
          const imported = parameterSummary(inputs[inputIndexes[0]]?.definition) ?? defaults;
          const existing = parameterSummary(rule.definition) ?? defaults;
          return (
            <li key={rule.rule_id}>
              <label className="flex cursor-pointer items-start gap-2 text-xs">
                <Checkbox
                  className="mt-0.5"
                  checked={reuse}
                  disabled={disabled}
                  onCheckedChange={(checked) => onToggle(rule.rule_id, checked)}
                  aria-label={t("similarRules.reuse", { name })}
                />
                <span className="space-y-0.5">
                  <span className="block font-medium">
                    {t("similarRules.reuse", { name })}
                    <span className="ml-1 font-normal opacity-70">
                      {t("similarRules.ruleState", { version: rule.version, status: rule.status })}
                    </span>
                  </span>
                  <span className="block opacity-80">
                    {t("similarRules.settings", { existing, imported })}
                  </span>
                  <span className="block opacity-80">
                    {reuse
                      ? t("similarRules.willReuse", { count: inputIndexes.length })
                      : t("similarRules.willCreate", { count: inputIndexes.length })}
                  </span>
                </span>
              </label>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
