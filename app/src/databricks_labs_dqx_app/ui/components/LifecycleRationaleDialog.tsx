/**
 * Shared dialog for Submit / Approve / Reject with an optional change rationale.
 * The note is stored on the lifecycle event (pending_rationale / last_decision_rationale),
 * not in the free-text comment thread.
 */
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
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
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";

export type LifecycleAction = "submit" | "approve" | "reject";

export function LifecycleRationaleDialog({
  open,
  onOpenChange,
  action,
  title,
  description,
  confirmLabel,
  requireRationale = false,
  destructive = false,
  busy = false,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  action: LifecycleAction;
  title: string;
  /** Optional subtitle under the title. Omitted when empty. */
  description?: string;
  confirmLabel: string;
  /** When true, Confirm stays disabled until rationale is non-empty. */
  requireRationale?: boolean;
  destructive?: boolean;
  busy?: boolean;
  onConfirm: (rationale: string | null) => void;
}) {
  const { t } = useTranslation();
  const [rationale, setRationale] = useState("");

  useEffect(() => {
    if (open) setRationale("");
  }, [open]);

  const trimmed = rationale.trim();
  const canConfirm = !requireRationale || trimmed.length > 0;

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          {description ? (
            <AlertDialogDescription>{description}</AlertDialogDescription>
          ) : (
            <AlertDialogDescription className="sr-only">{title}</AlertDialogDescription>
          )}
        </AlertDialogHeader>
        <div className="space-y-2 py-1">
          <Label htmlFor={`lifecycle-rationale-${action}`}>
            {t("lifecycle.rationaleLabel")}
            {requireRationale ? (
              <span className="text-destructive"> *</span>
            ) : (
              <span className="text-muted-foreground font-normal">
                {" "}
                ({t("lifecycle.rationaleOptional")})
              </span>
            )}
          </Label>
          <Textarea
            id={`lifecycle-rationale-${action}`}
            value={rationale}
            onChange={(e) => setRationale(e.target.value)}
            placeholder={t(`lifecycle.rationalePlaceholder.${action}`)}
            rows={3}
            className="text-sm"
            disabled={busy}
          />
        </div>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={busy}>{t("common.cancel")}</AlertDialogCancel>
          <AlertDialogAction
            disabled={!canConfirm || busy}
            className={destructive ? "bg-destructive text-white hover:bg-destructive/90" : undefined}
            onClick={(e) => {
              e.preventDefault();
              if (!canConfirm || busy) return;
              onConfirm(trimmed.length > 0 ? trimmed : null);
            }}
          >
            {confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
