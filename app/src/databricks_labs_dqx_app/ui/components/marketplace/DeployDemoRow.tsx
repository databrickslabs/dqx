import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import type { AxiosError } from "axios";
import { FlaskConical, Loader2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useDeployDemoContent,
  useDemoContentStatus,
  getDemoContentStatusQueryKey,
} from "@/lib/api";

export function DeployDemoRow() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { isAdmin } = usePermissions();
  const [open, setOpen] = useState(false);
  const [wipeFirst, setWipeFirst] = useState(true);
  const deployMutation = useDeployDemoContent();

  const { data: statusResp } = useDemoContentStatus({
    query: {
      refetchInterval: (query) => (query.state.data?.data?.state === "running" ? 10000 : false),
    },
  });
  const status = statusResp?.data;
  const isRunning = status?.state === "running";

  const closeDialog = () => setOpen(false);

  const handleConfirm = () => {
    if (deployMutation.isPending) return;
    deployMutation.mutate(
      { data: { wipe_first: wipeFirst } },
      {
        onSuccess: () => {
          toast.success(t("config.demoStarted"));
          closeDialog();
          queryClient.invalidateQueries({ queryKey: getDemoContentStatusQueryKey() });
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.demoFailed"));
        },
      },
    );
  };

  return (
    <>
      <button
        type="button"
        disabled={!isAdmin || isRunning}
        onClick={() => {
          setWipeFirst(true);
          setOpen(true);
        }}
        className={cn(
          "w-full flex items-start gap-3 rounded-lg border border-amber-500/40 bg-amber-500/5 px-4 py-3 text-left transition-colors hover:bg-amber-500/10 disabled:opacity-60 disabled:cursor-not-allowed",
        )}
      >
        <FlaskConical className="h-5 w-5 shrink-0 text-amber-600" aria-hidden />
        <div className="space-y-1">
          <div className="text-sm font-medium flex items-center gap-2">
            {t("config.demoTitle")}
            {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}
          </div>
          <p className="text-xs text-muted-foreground leading-relaxed">
            {isRunning ? t("config.demoRunningBanner", { phase: status?.phase ?? "" }) : t("config.demoBody")}
          </p>
        </div>
      </button>

      <Dialog
        open={open}
        onOpenChange={(o) => {
          if (deployMutation.isPending) return;
          if (o) setOpen(true);
          else closeDialog();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FlaskConical className="h-5 w-5" />
              {t("config.demoDialogTitle")}
            </DialogTitle>
            <DialogDescription>{t("config.demoWarning")}</DialogDescription>
          </DialogHeader>
          <div className="flex items-start gap-2">
            <Checkbox
              id="demo-wipe-first"
              checked={wipeFirst}
              onCheckedChange={(c) => setWipeFirst(c === true)}
              disabled={deployMutation.isPending}
            />
            <Label htmlFor="demo-wipe-first" className="text-xs leading-relaxed">
              {t("config.demoWipeLabel")}
            </Label>
          </div>
          <DialogFooter>
            <Button variant="ghost" size="sm" onClick={closeDialog} disabled={deployMutation.isPending}>
              {t("config.demoCancel")}
            </Button>
            <Button size="sm" onClick={handleConfirm} disabled={deployMutation.isPending} className="gap-1.5">
              {deployMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {t("config.demoConfirm")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
