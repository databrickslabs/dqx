import { useTranslation } from "react-i18next";
import { X } from "lucide-react";

/**
 * A row of removable filter chips. Renders nothing when there are no
 * filters. No data fetching — callers pass the active filters in.
 */
export function FilterChips({
  filters,
  onRemove,
}: {
  filters: Array<{ key: string; label: string }>;
  onRemove: (key: string) => void;
}) {
  const { t } = useTranslation();
  if (filters.length === 0) return null;

  return (
    <div className="flex flex-wrap items-center gap-2">
      {filters.map((f) => (
        <span
          key={f.key}
          className="bg-red-50 text-red-800 dark:bg-red-950/40 dark:text-red-300 rounded-full px-2.5 py-0.5 text-xs inline-flex items-center gap-1"
        >
          {f.label}
          <button
            type="button"
            aria-label={t("resultsUi.removeFilterAria", { label: f.label })}
            onClick={() => onRemove(f.key)}
            className="hover:opacity-70"
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
    </div>
  );
}
