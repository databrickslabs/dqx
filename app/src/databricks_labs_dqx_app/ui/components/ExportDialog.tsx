import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { AxiosResponse } from "axios";
import { toast } from "sonner";
import { FileDown, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { downloadExportFile, type ExportOut } from "@/lib/api-custom";
import { extractApiError } from "@/components/apply-rules/shared";

export type ExportFetcher = () => Promise<AxiosResponse<ExportOut>>;

interface ExportDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Fetches the DQX check-list export — always available. */
  fetchDqx: ExportFetcher;
  /** Fetches the ODCS DataContract export. Omit for DQX-only surfaces (rules
   *  have no physicalName to bind an ODCS contract to). */
  fetchOdcs?: ExportFetcher;
  /** Override the dialog title. Defaults to `exportYaml.modalTitle`. */
  title?: string;
}

/**
 * Controlled modal that lets the user choose an export format (DQX YAML or
 * ODCS data contract) before downloading.  The ODCS option is hidden when
 * *fetchOdcs* is not provided (rules-only surfaces).
 *
 * When there is only one available format (no *fetchOdcs*), the modal is
 * skipped entirely: opening it downloads the DQX export directly, preserving
 * the same success/error toasts (bug-bash-v4 item 3). The multi-format case
 * keeps the choice modal.
 *
 * The dialog is trigger-agnostic — callers manage *open* + *onOpenChange*.
 */
export function ExportDialog({ open, onOpenChange, fetchDqx, fetchOdcs, title }: ExportDialogProps) {
  const { t } = useTranslation();
  const [busy, setBusy] = useState(false);
  const singleFormat = !fetchOdcs;

  const run = useCallback(
    async (fetcher: ExportFetcher) => {
      setBusy(true);
      try {
        const res = await fetcher();
        downloadExportFile(res.data);
        toast.success(t("exportYaml.success", { filename: res.data.filename }));
        onOpenChange(false);
      } catch (err) {
        toast.error(extractApiError(err, t("exportYaml.failed")));
        onOpenChange(false);
      } finally {
        setBusy(false);
      }
    },
    [onOpenChange, t],
  );

  // Single-format surfaces have no choice to make, so "opening" the dialog
  // just triggers the download. We guard with a ref so a re-render while the
  // request is in flight doesn't fire a second download.
  const autoRunning = useRef(false);
  useEffect(() => {
    if (!singleFormat) return;
    if (open && !autoRunning.current) {
      autoRunning.current = true;
      void run(fetchDqx);
    } else if (!open) {
      autoRunning.current = false;
    }
  }, [open, singleFormat, run, fetchDqx]);

  // No modal for single-format surfaces — the effect above drives the
  // download directly.
  if (singleFormat) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>{title ?? t("exportYaml.modalTitle")}</DialogTitle>
          <DialogDescription>{t("exportYaml.modalDescription")}</DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2 pt-2">
          <Button
            variant="outline"
            className="justify-start gap-2"
            disabled={busy}
            onClick={() => void run(fetchDqx)}
          >
            {busy ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <FileDown className="h-4 w-4" />
            )}
            {t("exportYaml.dqxOption")}
          </Button>
          {fetchOdcs && (
            <Button
              variant="outline"
              className="justify-start gap-2"
              disabled={busy}
              onClick={() => void run(fetchOdcs)}
            >
              {busy ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <FileDown className="h-4 w-4" />
              )}
              {t("exportYaml.odcsOption")}
            </Button>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
