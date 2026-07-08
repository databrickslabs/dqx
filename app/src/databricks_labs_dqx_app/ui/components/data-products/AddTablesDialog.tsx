/**
 * AddTablesDialog — ported from dqlake's `components/products/AddTablesDialog.tsx`.
 *
 * Adapted to DQX's `DataProductMemberOut` shape (single `table_fqn`, real
 * `binding_id`) and the app's `MonitoredTableSummaryOut` picker rows, whose
 * `table.version` drives the per-pick pin dropdown exactly like dqlake's
 * picker row `version` did. In DQX a pin REALLY executes that frozen
 * version's checks (design spec §3.4) — the header copy says so.
 */
import { Suspense, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { RotateCcw } from "lucide-react";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { type DataProductMemberOut, type MonitoredTableSummaryOut } from "@/lib/api";
import { MemberVersionPin } from "@/components/data-products/MemberVersionPin";
import { TablesPicker } from "@/components/data-products/TablesPicker";

interface Props {
  existingMembers: DataProductMemberOut[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Buffer the picked tables as new members. */
  onAdd: (members: DataProductMemberOut[]) => void;
}

function Inner({ existingMembers, onOpenChange, onAdd }: Omit<Props, "open">) {
  const { t } = useTranslation();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // Per-table version pin choice keyed by binding_id. Absent = use latest (null).
  const [pins, setPins] = useState<Map<string, number | null>>(new Map());
  // binding_id -> the loaded picker row, so we can carry FQN/status/counts
  // onto the buffered member.
  const [rowByBindingId, setRowByBindingId] = useState<Map<string, MonitoredTableSummaryOut>>(new Map());

  const existingMemberIds = useMemo(() => {
    const s = new Set<string>();
    for (const m of existingMembers) s.add(m.binding_id);
    return s;
  }, [existingMembers]);

  const handleRowsLoaded = (rows: MonitoredTableSummaryOut[]) => {
    setRowByBindingId(new Map(rows.map((r) => [r.table.binding_id, r])));
  };

  const picks = useMemo(() => [...selected].filter((k) => !existingMemberIds.has(k)), [selected, existingMemberIds]);

  function handleConfirm() {
    if (picks.length === 0) return;
    // New members have no server id yet — flushed on Save via
    // useEditProductState's member-buffer reconcile. The binding's
    // version and status come from the loaded picker row, so the Tables
    // tab shows the correct pin badge immediately; pinned_version
    // defaults to "use latest" (null) unless picked below.
    const newMembers: DataProductMemberOut[] = picks.map((bindingId) => {
      const row = rowByBindingId.get(bindingId);
      const version = row?.table.version ?? 0;
      return {
        id: "",
        binding_id: bindingId,
        table_fqn: row?.table.table_fqn ?? bindingId,
        binding_status: row?.table.status ?? "draft",
        binding_version: version,
        pinned_version: pins.get(bindingId) ?? null,
        rules_count: row?.applied_rule_count ?? 0,
        checks_count: row?.check_count ?? 0,
        // Matches the backend's definition: approved AND version > 0.
        runnable: row?.table.status === "approved" && version > 0,
      };
    });
    onAdd(newMembers);
    onOpenChange(false);
  }

  const count = picks.length;

  return (
    <>
      <DialogHeader>
        <DialogTitle className="text-sm font-semibold">{t("dataProducts.addTablesTitle")}</DialogTitle>
        <p className="text-xs text-muted-foreground">{t("dataProducts.addTablesDescription")}</p>
      </DialogHeader>

      <div className="flex-1 overflow-y-auto min-h-0 max-h-[60vh]">
        <TablesPicker
          selected={selected}
          onChange={setSelected}
          disabledKeys={existingMemberIds}
          onRowsLoaded={handleRowsLoaded}
        />
      </div>

      {count > 0 && (
        <div className="border-t pt-3 space-y-1.5 max-h-[22vh] overflow-y-auto">
          <p className="text-xs font-medium text-muted-foreground">{t("dataProducts.versionToTrackLabel")}</p>
          {picks.map((bindingId) => {
            const row = rowByBindingId.get(bindingId);
            const fqn = row?.table.table_fqn ?? bindingId;
            return (
              <div key={bindingId} className="flex items-center justify-between gap-3">
                <span className="font-mono text-xs truncate">{fqn}</span>
                <MemberVersionPin
                  bindingVersion={row?.table.version ?? 0}
                  pinnedVersion={pins.get(bindingId) ?? null}
                  onPinChange={(ver) =>
                    setPins((prev) => {
                      const next = new Map(prev);
                      next.set(bindingId, ver);
                      return next;
                    })
                  }
                />
              </div>
            );
          })}
        </div>
      )}

      <DialogFooter className="flex-col sm:flex-row items-center gap-2 border-t pt-4 sm:justify-end">
        <Button variant="outline" size="sm" onClick={() => onOpenChange(false)}>
          {t("common.cancel")}
        </Button>
        <Button size="sm" onClick={() => handleConfirm()} disabled={count === 0}>
          {t("dataProducts.addTablesConfirm", { count })}
        </Button>
      </DialogFooter>
    </>
  );
}

export function AddTablesDialog({ existingMembers, open, onOpenChange, onAdd }: Props) {
  const { t } = useTranslation();
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="!max-w-[min(90vw,1100px)] w-[min(90vw,1100px)] max-h-[85vh] flex flex-col gap-4 p-6"
        onInteractOutside={(e) => {
          // The per-table "Version to track" pin (MemberVersionPin) is a
          // Radix DropdownMenu whose menu renders in a popper portal that is
          // a sibling of — not a descendant of — this dialog's content. A
          // click on a menu item can therefore register as an interaction
          // *outside* the dialog and dismiss the whole dialog instead of
          // just closing the pin menu. Ignore any outside-interaction that
          // originates inside a Radix popper portal (menu/select/popover) so
          // picking a version only updates the pin and leaves the dialog open.
          const target = e.detail.originalEvent.target as Element | null;
          if (target?.closest("[data-radix-popper-content-wrapper],[role=menu]")) {
            e.preventDefault();
          }
        }}
      >
        <QueryErrorResetBoundary>
          {({ reset }) => (
            <ErrorBoundary
              onReset={reset}
              fallbackRender={({ resetErrorBoundary }) => (
                <div className="flex flex-col gap-4 py-8 text-center text-sm text-muted-foreground">
                  <p>{t("dataProducts.addTablesLoadFailed")}</p>
                  <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2 mx-auto">
                    <RotateCcw className="h-3 w-3" />
                    {t("common.retry")}
                  </Button>
                </div>
              )}
            >
              <Suspense
                fallback={
                  <div className="space-y-3 pt-4">
                    <Skeleton className="h-8 w-full" />
                    <Skeleton className="h-64 w-full" />
                  </div>
                }
              >
                <Inner existingMembers={existingMembers} onOpenChange={onOpenChange} onAdd={onAdd} />
              </Suspense>
            </ErrorBoundary>
          )}
        </QueryErrorResetBoundary>
      </DialogContent>
    </Dialog>
  );
}
