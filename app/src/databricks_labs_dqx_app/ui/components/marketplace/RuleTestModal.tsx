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

  const mode = (preview?.mode ?? "sql") as "sql" | "lowcode" | "dqx_native";
  const polarity: "pass" | "fail" = preview?.polarity === "fail" ? "fail" : "pass";
  const slots = def?.slots ?? [];

  // Read the ORIGINAL normalized check ({criticality, check:{function,
  // arguments}, user_metadata}), NOT the parsed preview definition — the
  // parser splits scalar args like `regex` / `allowed` out into
  // `parameters`, so the preview body drops them. The test panel's native
  // path needs the FULL arguments (function + regex/allowed/min_limit/…),
  // exactly as the editor's Test tab passes them.
  const innerCheck =
    rule.check && typeof rule.check === "object"
      ? ((rule.check as Record<string, unknown>).check as Record<string, unknown> | undefined)
      : undefined;
  const nativeFunction = typeof innerCheck?.function === "string" ? innerCheck.function : undefined;
  const nativeArguments =
    innerCheck?.arguments && typeof innerCheck.arguments === "object" && !Array.isArray(innerCheck.arguments)
      ? (innerCheck.arguments as Record<string, unknown>)
      : {};

  // sql_expression rules carry their predicate in arguments.expression; sql
  // rules may carry sql_query/predicate. dqx_native rules compile on the
  // backend from function + arguments, so their predicate is unused.
  const predicate =
    (typeof nativeArguments.expression === "string" && nativeArguments.expression) ||
    (typeof nativeArguments.sql_query === "string" && nativeArguments.sql_query) ||
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
