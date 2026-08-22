import type { ReactNode } from "react";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";

interface BulkActionBarProps {
  /** Number of selected rows. The bar renders only when this is > 0. */
  count: number;
  /** Pre-translated "{{count}} selected" label (namespaced per surface). */
  label: ReactNode;
  /** When true, replaces the action buttons with a spinner. */
  busy?: boolean;
  /** Clears the current selection (hides the bar). */
  onClear: () => void;
  /** Pre-translated "Clear selection" label. */
  clearLabel: string;
  /** Surface-specific action buttons, rendered in canonical order. */
  children?: ReactNode;
}

/**
 * Selection action bar shared by the Rules Registry, Monitored Tables, and
 * Table Spaces overviews (bug-bash-v4 item 12). It is positioned as an
 * **overlay** over the filter/search toolbar row (the parent must be
 * `relative`) so that selecting rows never shifts the table down a row — the
 * bar covers the filters while a selection is active and vanishes when
 * cleared, leaving the table in exactly the same position.
 *
 * The count label and clear label are passed pre-translated so each surface
 * keeps its own i18n namespace; the action buttons are provided as *children*
 * in the canonical order (Run → Approve → Reject → [Deprecate → Revoke] →
 * Export → Delete), with Clear pinned to the right.
 */
export function BulkActionBar({ count, label, busy, onClear, clearLabel, children }: BulkActionBarProps) {
  if (count <= 0) return null;
  // min-h matches the filter toolbar row height (h-8 controls + py-2 = ~48px)
  // so the overlay can never clip into the column headers if spacing changes.
  return (
    <div className="absolute inset-x-0 top-0 z-10 flex flex-wrap items-center gap-2 rounded-lg border bg-muted py-2 px-2.5 min-h-[48px]">
      <span className="text-sm font-medium mr-1">{label}</span>
      {busy ? (
        <Loader2 className="h-4 w-4 animate-spin" />
      ) : (
        <>
          {children}
          <Button size="sm" variant="ghost" className="h-7 text-xs ml-auto" onClick={onClear}>
            {clearLabel}
          </Button>
        </>
      )}
    </div>
  );
}
