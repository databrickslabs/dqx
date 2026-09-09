/**
 * Asks for the run scope (full table or an N-row sample) before a run is
 * submitted, and reports the resolved `sample_size` the run endpoints take
 * (0 = full table).
 *
 * Used by every MANUAL run affordance — "Run now" and "Run draft", on both a
 * monitored table and a collection. Scheduled runs never sample (the backend
 * forces a full table for them), so they never open this.
 */
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Loader2, Play } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { SampleSelector, type SampleKind } from "@/components/rules/test/RuleTestPanel";

/** Mirrors the `sample_size` upper bound on the run endpoints. */
const MAX_SAMPLE_ROWS = 10_000_000;

export function RunSampleDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel,
  busy = false,
  /** Scope the dialog opens on. "full" for published runs (a sample is the
   *  deliberate exception there); "records" for draft runs, which are cheap
   *  spot-checks by default. */
  defaultKind = "full",
  defaultValue = 1000,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: string;
  confirmLabel: string;
  busy?: boolean;
  defaultKind?: SampleKind;
  defaultValue?: number;
  /** Receives the resolved sample size: 0 for a full table, else the row count. */
  onConfirm: (sampleSize: number) => void;
}) {
  const { t } = useTranslation();
  const [kind, setKind] = useState<SampleKind>(defaultKind);
  const [value, setValue] = useState(defaultValue);

  // Re-seed on every open so a cancelled edit never leaks into the next run.
  useEffect(() => {
    if (open) {
      setKind(defaultKind);
      setValue(defaultValue);
    }
  }, [open, defaultKind, defaultValue]);

  // Clamped to the run endpoints' own bounds (1..10M rows) so a cleared or
  // over-large input can't turn a confirm into a 422. A blank field reads as
  // NaN, which falls back to the seeded default.
  const rows = Number.isFinite(value) ? Math.min(MAX_SAMPLE_ROWS, Math.max(1, Math.floor(value))) : defaultValue;
  const sampleSize = kind === "full" ? 0 : rows;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <div className="py-1">
          <SampleSelector kind={kind} value={value} onKind={setKind} onValue={setValue} disablePercent />
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={busy}>
            {t("common.cancel")}
          </Button>
          <Button className="gap-2" disabled={busy} onClick={() => onConfirm(sampleSize)}>
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            {confirmLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
