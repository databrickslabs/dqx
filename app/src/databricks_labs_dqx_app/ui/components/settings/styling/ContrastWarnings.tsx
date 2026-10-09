import { useTranslation } from "react-i18next";
import { AlertTriangle } from "lucide-react";
import type { ContrastWarning } from "@/lib/branding";

export function ContrastWarnings({ warnings }: { warnings: ContrastWarning[] }) {
  const { t } = useTranslation();
  if (warnings.length === 0) return null;
  return (
    <div role="status" className="rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm">
      <div className="flex items-center gap-2 font-medium text-amber-700 dark:text-amber-400">
        <AlertTriangle className="h-4 w-4 shrink-0" />
        {t("config.styling.contrastTitle")}
      </div>
      <ul className="mt-2 list-disc space-y-1 pl-6 text-xs">
        {warnings.map((w) => (
          <li key={`${w.mode}-${w.pair}`}>
            {t("config.styling.contrastItem", {
              mode: t(w.mode === "light" ? "config.styling.modeLight" : "config.styling.modeDark"),
              pair: t(`config.styling.pair_${w.pair}`),
              ratio: w.ratio,
              min: w.min,
            })}
          </li>
        ))}
      </ul>
    </div>
  );
}
