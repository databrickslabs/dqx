import { useEffect, useState } from "react";
import { X } from "lucide-react";
import { useTranslation } from "react-i18next";

const LS_KEY = "dqx.registry-sql-explainer-dismissed";

/**
 * Compact, dismissible mini explainer card shown above the SQL predicate
 * editor. Ported from dqlake's `PredicateEditorExplainer` — the dismiss
 * choice persists in localStorage so it doesn't nag returning authors.
 */
export function PredicateEditorExplainer() {
  const { t } = useTranslation();
  const [dismissed, setDismissed] = useState(true);

  useEffect(() => {
    setDismissed(window.localStorage.getItem(LS_KEY) === "1");
  }, []);

  if (dismissed) return null;

  const dismiss = () => {
    window.localStorage.setItem(LS_KEY, "1");
    setDismissed(true);
  };

  return (
    <div className="flex items-start gap-2 text-xs text-muted-foreground border rounded-md px-3 py-2 bg-muted/30">
      <div className="flex-1">
        {t("rulesRegistry.sqlExplainerText")} <code className="font-mono">{`{{slot_name}}`}</code>{" "}
        {t("rulesRegistry.sqlExplainerTextSuffix")}
      </div>
      <button type="button" onClick={dismiss} aria-label={t("rulesRegistry.sqlExplainerDismiss")}>
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
