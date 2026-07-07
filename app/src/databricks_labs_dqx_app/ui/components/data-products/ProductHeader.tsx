/**
 * Ported from dqlake's `products/$productId.tsx`'s `ProductHeader` section.
 * Adapted: no `SyncStatusChip`/`sync_state` concept (DQX runs stay in-app,
 * there is no external Databricks-side reconcile step to poll); RBAC gates
 * edit actions on `canEdit` (RULE_AUTHOR+) and run actions on `canRun`
 * (RUNNER, orthogonal) rather than dqlake's single per-object `can_edit`;
 * "Run draft" moved into the ⋮ menu per the design spec since Runs is now a
 * visible tab (dqlake tucks the whole Runs surface there instead).
 */
import { useEffect, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  useDeleteDataProduct,
  useRunDataProduct,
  RunDataProductInSource,
  type DataProductOut,
} from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";
import { useProductRunSets } from "@/hooks/use-product-run-sets";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Loader2, MoreVertical, Play, Save, Send, Trash2 } from "lucide-react";
import { cn } from "@/lib/utils";
import type { EditProductState } from "@/components/data-products/useEditProductState";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

interface Props {
  product: DataProductOut;
  canEdit: boolean;
  editState: EditProductState;
}

export function ProductHeader({ product, canEdit, editState }: Props) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const perms = usePermissions();
  const canRun = perms.canRunRules;

  const runMut = useRunDataProduct({ mutation: { onError: () => {} } });
  const deleteMut = useDeleteDataProduct({ mutation: { onError: () => {} } });

  const [deleteOpen, setDeleteOpen] = useState(false);
  const [busyRun, setBusyRun] = useState(false);
  // Bridges the gap between a successful submit and the next 4s poll
  // catching the new RUNNING run set, so the button doesn't flash back to
  // "Run now" for a moment after submission.
  const [justSubmitted, setJustSubmitted] = useState(false);
  // Shares its `listRunSets` query with `ProductRunsTab` (identical params)
  // so the two don't each poll the endpoint independently.
  const { hasActive } = useProductRunSets(product.product_id, { extraPoll: justSubmitted });
  useEffect(() => {
    if (hasActive) setJustSubmitted(false);
  }, [hasActive]);

  // Safety net: a submit can return 200 without ever producing an active run
  // set (e.g. the backend accepted the request but every member failed to
  // launch, so the run set was rolled back). Without this, `justSubmitted`
  // would keep the button on "Running…" forever. Clear it if no active run
  // set confirms within 30s so the button always returns to a terminal state.
  useEffect(() => {
    if (!justSubmitted) return;
    const id = window.setTimeout(() => setJustSubmitted(false), 30_000);
    return () => window.clearTimeout(id);
  }, [justSubmitted]);

  const runPending = busyRun || hasActive || justSubmitted;

  // Publish-only state: a saved draft with no in-memory edits and a prior
  // publish on record — the only relevant action is Publish. Hiding
  // "Save & publish" here avoids a greyed-out button implying "save
  // something first" when nothing is unsaved.
  const inPublishOnlyState = !editState.isDirty && product.status === "draft" && product.version > 0;

  const runnableCount = product.runnable_count ?? 0;
  const memberCount = product.member_count ?? 0;

  const handleRun = async (source: (typeof RunDataProductInSource)[keyof typeof RunDataProductInSource]) => {
    setBusyRun(true);
    try {
      const resp = await runMut.mutateAsync({ productId: product.product_id, data: { source } });
      // The run endpoint returns 200 even when EVERY member failed to launch
      // (their failures collected into `skipped`, `submitted` left empty, and
      // the empty run set rolled back). That is not a started run, so treat an
      // empty `submitted` as a failure: surface it and do NOT enter the
      // "Running…" bridge state, which would otherwise stick forever since no
      // active run set will ever confirm it.
      if ((resp.data.submitted?.length ?? 0) === 0) {
        toast.error(t("dataProducts.toastRunNoneStarted"), { duration: 6000 });
        return;
      }
      setJustSubmitted(true);
      toast.success(t("dataProducts.toastRunStarted"));
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastRunFailed")), { duration: 6000 });
    } finally {
      setBusyRun(false);
    }
  };

  const handleDelete = async () => {
    try {
      await deleteMut.mutateAsync({ productId: product.product_id });
      toast.success(t("dataProducts.toastDeleted"));
      void navigate({ to: "/data-products" });
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastDeleteFailed")), { duration: 6000 });
    }
  };

  const showMenu = canRun || canEdit;

  return (
    <div className="flex items-start justify-between gap-4 border-b pb-4 max-w-5xl">
      <div className="space-y-1">
        <div className="flex items-center gap-2">
          <h1 className="text-xl font-semibold">{product.name}</h1>
          {product.status === "published" && <Badge>{t("dataProducts.versionBadge", { version: product.version })}</Badge>}
        </div>
      </div>
      <div className="flex gap-2 items-center">
        {canEdit && (
          <Button
            onClick={() => void editState.handleSaveDraft()}
            disabled={!editState.canSave || editState.savePending}
            variant="outline"
            size="sm"
            className="gap-2"
          >
            {editState.savePending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            {t("dataProducts.saveAsDraftButton")}
          </Button>
        )}

        {canEdit && !inPublishOnlyState && (
          <Button
            onClick={() => void editState.handleSaveAndPublish()}
            disabled={!editState.canSave || editState.saveAndPublishPending}
            variant="outline"
            size="sm"
            className="gap-2"
          >
            {editState.saveAndPublishPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            {t("dataProducts.saveAndPublishButton")}
          </Button>
        )}

        {canEdit && inPublishOnlyState && (
          <Button
            onClick={() => void editState.handlePublish()}
            disabled={editState.publishPending}
            size="sm"
            className="gap-2"
          >
            {editState.publishPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            {t("dataProducts.publishButton")}
          </Button>
        )}

        {canRun && (
          <Button
            onClick={() => void handleRun(RunDataProductInSource.approved)}
            disabled={runPending || runnableCount === 0}
            size="sm"
            className="gap-2"
            title={runnableCount === 0 ? t("dataProducts.runNowDisabledHint") : undefined}
          >
            {runPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            {runPending ? t("dataProducts.runningLabel") : t("dataProducts.runNowButton")}
          </Button>
        )}

        {showMenu && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button size="sm" variant="ghost" aria-label={t("dataProducts.actionsMenuLabel")}>
                <MoreVertical className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {canRun && (
                <DropdownMenuItem
                  onSelect={() => void handleRun(RunDataProductInSource.draft)}
                  disabled={runPending || memberCount === 0}
                  className="gap-2"
                >
                  <Play className="h-3.5 w-3.5" />
                  {t("dataProducts.runDraftAction")}
                </DropdownMenuItem>
              )}
              {canRun && canEdit && <DropdownMenuSeparator />}
              {canEdit && (
                <DropdownMenuItem
                  onSelect={() => setDeleteOpen(true)}
                  className={cn("gap-2 text-destructive focus:text-destructive")}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                  {t("dataProducts.deleteAction")}
                </DropdownMenuItem>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("dataProducts.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>{t("dataProducts.deleteConfirmDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteMut.isPending}>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className={cn("bg-destructive text-white hover:bg-destructive/90")}
              disabled={deleteMut.isPending}
              onClick={(e) => {
                e.preventDefault();
                setDeleteOpen(false);
                void handleDelete();
              }}
            >
              {t("dataProducts.deleteAction")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
