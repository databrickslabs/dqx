// Selection-only "Add rules" dialog for the policy form: the real RulesPicker in a Dialog.
// Drafts and pending rules can be picked; their policies attach once the rule is approved.
import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { RulesPicker } from "@/components/apply-rules/RulesPicker";
import type { LabelColorDefinition } from "@/components/RegistryRuleBadges";
import type { RegistryRuleOut } from "@/lib/api";

export function PolicyRulesDialog({
  open,
  onOpenChange,
  rules,
  alreadyAddedIds,
  labelDefinitions,
  isLoading,
  isError,
  onRetry,
  onAdd,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Rules that may be added (the policy's own rule already excluded). */
  rules: RegistryRuleOut[];
  /** Rules already in the Rules section: shown checked and locked. */
  alreadyAddedIds: Set<string>;
  labelDefinitions: LabelColorDefinition[];
  isLoading?: boolean;
  isError?: boolean;
  onRetry?: () => void;
  onAdd: (rules: RegistryRuleOut[]) => void;
}) {
  const { t } = useTranslation();
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const close = (next: boolean) => {
    if (!next) setSelected(new Set());
    onOpenChange(next);
  };

  const toggle = (rule: RegistryRuleOut) => {
    if (alreadyAddedIds.has(rule.rule_id)) return;
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(rule.rule_id)) next.delete(rule.rule_id);
      else next.add(rule.rule_id);
      return next;
    });
  };

  const rowNote = useCallback(
    (rule: RegistryRuleOut) => (rule.status === "approved" ? null : t("rulePolicies.picker.appliesOnceApproved")),
    [t],
  );

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent className="w-[min(92vw,900px)] !max-w-[min(92vw,900px)]">
        <DialogHeader>
          <DialogTitle>{t("rulePolicies.picker.title")}</DialogTitle>
          <DialogDescription>{t("rulePolicies.picker.description")}</DialogDescription>
        </DialogHeader>
        <div className="min-w-0 space-y-3 px-2">
          <RulesPicker
            rules={rules}
            labelDefinitions={labelDefinitions}
            selectedIds={selected}
            appliedIds={alreadyAddedIds}
            onToggle={toggle}
            isLoading={isLoading}
            isError={isError}
            onRetry={onRetry}
            rowNote={rowNote}
          />
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => close(false)}>
            {t("common.cancel")}
          </Button>
          <Button
            disabled={selected.size === 0}
            onClick={() => {
              onAdd(rules.filter((r) => selected.has(r.rule_id)));
              close(false);
            }}
          >
            {selected.size === 0
              ? t("rulePolicies.picker.title")
              : t("rulePolicies.picker.add", { count: selected.size })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
