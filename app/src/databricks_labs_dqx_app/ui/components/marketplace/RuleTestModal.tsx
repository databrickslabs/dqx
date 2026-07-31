import { useTranslation } from "react-i18next";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import type { CheckFunctionDef, MarketplaceRuleOut, RuleDefinition } from "@/lib/api";
import { RuleTestPanel } from "@/components/rules/test/RuleTestPanel";
import { checkDictToPreviewRule } from "@/lib/marketplace-selection";

/**
 * "Try it out" — runs a marketplace rule through the EXISTING RuleTestPanel
 * (the same component the rule editor's Test tab uses), so the fill-in grid,
 * AI sample-data generation, table sampling, pass/fail highlighting, and
 * warehouse handling are all reused verbatim. We only translate the pack
 * rule's stored check into the panel's props via the shared preview-rule
 * conversion — no test logic is re-implemented here.
 */
export function RuleTestModal({
  rule,
  checkFunctions,
  open,
  onOpenChange,
}: {
  rule: MarketplaceRuleOut;
  checkFunctions: CheckFunctionDef[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { t } = useTranslation();
  const preview = checkDictToPreviewRule(rule, checkFunctions, t);
  const def: RuleDefinition | undefined = preview?.definition;
  const body = (def?.body ?? {}) as Record<string, unknown>;

  const mode = (preview?.mode ?? "sql") as "sql" | "lowcode" | "dqx_native";
  const polarity: "pass" | "fail" = preview?.polarity === "fail" ? "fail" : "pass";
  const slots = def?.slots ?? [];

  const nativeFunction = typeof body.function === "string" ? body.function : undefined;
  const nativeArguments =
    body.arguments && typeof body.arguments === "object" && !Array.isArray(body.arguments)
      ? (body.arguments as Record<string, unknown>)
      : {};

  // For sql/lowcode rules the effective predicate is the stored predicate /
  // sql_query. dqx_native rules pass function + arguments and compile on the
  // backend, so their predicate is unused.
  const predicate =
    (typeof body.sql_query === "string" && body.sql_query) ||
    (typeof body.predicate === "string" && body.predicate) ||
    "";

  const canTest =
    mode === "dqx_native"
      ? Boolean(nativeFunction) && checkFunctions.some((f) => f.name === nativeFunction && f.rule_testable)
      : predicate.trim().length > 0;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>{t("marketplace.tryItTitle", { name: rule.name })}</DialogTitle>
        </DialogHeader>
        {preview ? (
          <RuleTestPanel
            predicate={predicate}
            polarity={polarity}
            slots={slots}
            ruleMode={mode}
            nativeFunction={mode === "dqx_native" ? nativeFunction : undefined}
            nativeArguments={mode === "dqx_native" ? nativeArguments : undefined}
            canTest={canTest}
          />
        ) : (
          <p className="text-sm italic text-muted-foreground">
            {t("monitoredTables.ruleLogicUnavailable")}
          </p>
        )}
      </DialogContent>
    </Dialog>
  );
}
