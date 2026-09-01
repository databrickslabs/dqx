import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { RelativeTimeCell } from "@/components/data-table/RelativeTimeCell";
import type { DataProductOut } from "@/lib/api";

/**
 * History tab for the table-space detail page — ported from dqlake's
 * `ProductHistoryTab` (right-aligned next to Schedule in the tab strip).
 *
 * dqlake feeds this from `DataProductSnapshot` rows (one audit entry per
 * CREATE/UPDATE/PUBLISH with the acting user and frozen state). DQX has no
 * data-product snapshot table — `dq_data_products` carries only the current
 * `version` plus created/updated audit stamps — so this renders the events
 * we do track, in dqlake's card style: a Created entry, a Last-updated
 * entry when the row has been touched since creation, and the current
 * published version called out above the feed.
 */
type HistoryEntry = {
  action: "created" | "updated";
  actor: string | null;
  ts: string;
};

// dqlake's BUCKET_CLASSES — Created maps to the CREATE bucket (emerald),
// Updated to the UPDATE bucket (amber).
const ACTION_CLASSES: Record<HistoryEntry["action"], string> = {
  created: "text-emerald-600 dark:text-emerald-400",
  updated: "text-amber-600 dark:text-amber-400",
};

function HistoryEntryCard({ entry }: { entry: HistoryEntry }) {
  const { t } = useTranslation();
  const label =
    entry.action === "created"
      ? t("dataProducts.historyActionCreated")
      : t("dataProducts.historyActionUpdated");

  return (
    <Card>
      <CardContent className="p-4 text-sm space-y-2">
        <div className="flex items-start justify-between gap-3">
          <span
            className={`font-mono text-xs uppercase tracking-[0.12em] font-semibold ${ACTION_CLASSES[entry.action]}`}
          >
            {label}
          </span>
          <span className="text-[11px] font-mono text-muted-foreground shrink-0">
            <RelativeTimeCell iso={entry.ts} />
          </span>
        </div>
        {entry.actor && <div className="font-mono text-xs text-muted-foreground">{entry.actor}</div>}
      </CardContent>
    </Card>
  );
}

export function ProductHistoryTab({ product }: { product: DataProductOut }) {
  const { t } = useTranslation();

  // Newest first, like dqlake's feed.
  const entries: HistoryEntry[] = [];
  if (product.updated_at && product.updated_at !== product.created_at) {
    entries.push({ action: "updated", actor: product.updated_by ?? null, ts: product.updated_at });
  }
  if (product.created_at) {
    entries.push({ action: "created", actor: product.created_by ?? null, ts: product.created_at });
  }

  if (entries.length === 0) {
    return <p className="text-sm text-muted-foreground">{t("dataProducts.historyEmpty")}</p>;
  }

  return (
    <div className="space-y-3 max-w-3xl">
      {product.version > 0 && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>{t("dataProducts.historyCurrentVersion")}</span>
          <Badge variant="secondary" className="font-mono text-[10px]">
            {t("dataProducts.versionBadge", { version: product.version })}
          </Badge>
        </div>
      )}
      {entries.map((e) => (
        <HistoryEntryCard key={e.action} entry={e} />
      ))}
    </div>
  );
}
