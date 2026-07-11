/**
 * ProductTablesTab — ported from dqlake's `components/products/ProductTablesTab.tsx`.
 *
 * Adapted to DQX's flatter member shape (`DataProductMemberOut` carries a
 * single `table_fqn` plus `rules_count`/`checks_count`/`runnable`, no
 * catalog/schema split) and the app's own monitored-table detail
 * route/tabs. The per-member DQ Score column (P5.3) matches dqlake's:
 * the cached table-scope score rendered with the shared `ScoreBarCell`
 * (same bar as the Monitored Tables / Table Spaces lists), linking to the
 * member's Results tab. Per the Data Products design spec §8, dqlake's
 * "Average score" summary footer row remains a sanctioned cut — the
 * space's own cached score is already shown on the list page.
 */
import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Plus, X } from "lucide-react";
import { type DataProductMemberOut } from "@/lib/api";
import type { EditProductState } from "@/components/data-products/useEditProductState";
import { ScoreBarCell } from "@/components/data-table/ScoreBarCell";
import { AddTablesDialog } from "@/components/data-products/AddTablesDialog";
import { MemberVersionPin } from "@/components/data-products/MemberVersionPin";

interface Props {
  editState: EditProductState;
  canEdit: boolean;
}

export function ProductTablesTab({ editState, canEdit }: Props) {
  const { t } = useTranslation();
  const [addOpen, setAddOpen] = useState(false);
  const [confirmMember, setConfirmMember] = useState<DataProductMemberOut | null>(null);

  const members = editState.members;

  const handleRemove = (member: DataProductMemberOut) => {
    setConfirmMember(null);
    editState.removeMember(member);
  };

  return (
    <div className="space-y-4 max-w-3xl">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-semibold">{t("dataProducts.tabTables")}</h2>
        {canEdit && members.length > 0 && (
          <Button size="sm" onClick={() => setAddOpen(true)}>
            <Plus className="h-4 w-4 mr-1" /> {t("dataProducts.addTablesButton")}
          </Button>
        )}
      </div>

      {members.length === 0 ? (
        <div className="flex flex-col items-center gap-4 py-12 text-center">
          <p className="text-sm text-muted-foreground">{t("dataProducts.tablesEmptyState")}</p>
          {canEdit && (
            <Button size="sm" onClick={() => setAddOpen(true)}>
              <Plus className="h-4 w-4 mr-1" /> {t("dataProducts.addTablesButton")}
            </Button>
          )}
        </div>
      ) : (
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t("monitoredTables.colTableName")}</TableHead>
                <TableHead className="w-[90px] text-right">{t("dataProducts.colRules")}</TableHead>
                <TableHead className="w-[90px] text-right">{t("dataProducts.colChecks")}</TableHead>
                <TableHead className="w-[160px]">{t("dataProducts.colDqScore")}</TableHead>
                <TableHead className="w-[150px]">{t("dataProducts.colVersion")}</TableHead>
                {canEdit && <TableHead className="w-[48px]" />}
              </TableRow>
            </TableHeader>
            <TableBody>
              {members.map((m) => (
                <TableRow key={m.binding_id} className="group">
                  <TableCell className="font-mono text-xs">
                    <Link
                      to="/monitored-tables/$bindingId"
                      params={{ bindingId: m.binding_id }}
                      search={{ tab: "about" }}
                      className="hover:underline"
                    >
                      {m.table_fqn}
                    </Link>
                    {!m.runnable && (
                      <p className="text-muted-foreground font-sans not-italic mt-0.5">
                        {t("dataProducts.memberNotReadyHelper")}
                      </p>
                    )}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <Link
                      to="/monitored-tables/$bindingId"
                      params={{ bindingId: m.binding_id }}
                      search={{ tab: "apply-rules" }}
                      className="hover:underline"
                    >
                      {m.rules_count}
                    </Link>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    <Link
                      to="/monitored-tables/$bindingId"
                      params={{ bindingId: m.binding_id }}
                      search={{ tab: "apply-rules" }}
                      className="hover:underline"
                    >
                      {m.checks_count}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <Link
                      to="/monitored-tables/$bindingId"
                      params={{ bindingId: m.binding_id }}
                      search={{ tab: "results" }}
                      className="hover:underline"
                    >
                      <ScoreBarCell score={m.score} />
                    </Link>
                  </TableCell>
                  <TableCell>
                    <MemberVersionPin
                      bindingVersion={m.binding_version ?? 0}
                      pinnedVersion={m.pinned_version ?? null}
                      onPinChange={canEdit ? (v) => editState.setMemberPin(m, v) : undefined}
                    />
                  </TableCell>
                  {canEdit && (
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7 opacity-0 group-hover:opacity-100"
                        aria-label={t("dataProducts.removeTableAria", { table: m.table_fqn })}
                        onClick={() => setConfirmMember(m)}
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </TableCell>
                  )}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {canEdit && (
        <AddTablesDialog
          existingMembers={members}
          open={addOpen}
          onOpenChange={setAddOpen}
          onAdd={(picked) => picked.forEach((m) => editState.addMember(m))}
        />
      )}

      <AlertDialog
        open={confirmMember !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmMember(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.removeTableConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("dataProducts.removeTableConfirmDescription", { table: confirmMember?.table_fqn ?? "" })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault();
                if (confirmMember) handleRemove(confirmMember);
              }}
            >
              {t("dataProducts.removeAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
