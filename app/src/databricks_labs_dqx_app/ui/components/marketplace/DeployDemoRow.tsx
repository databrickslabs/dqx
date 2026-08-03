import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import type { AxiosError } from "axios";
import { Rocket, Loader2 } from "lucide-react";
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
      {/* Inverse-toned banner (dark in light mode, light in dark mode) so it
          reads as a distinct, invitational setup action rather than a warning.
          The whole row is clickable; the Deploy button makes that affordance
          explicit. Icon + button are vertically centred against the text. */}
      <button
        type="button"
        disabled={!isAdmin || isRunning}
        onClick={() => {
          setWipeFirst(true);
          setOpen(true);
        }}
        className={cn(
          "w-full flex items-center gap-4 rounded-lg bg-foreground px-4 py-3.5 text-left text-background",
          "transition-opacity hover:opacity-90 disabled:opacity-60 disabled:cursor-not-allowed",
        )}
      >
        <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-background/15">
          <Rocket className="h-5 w-5" aria-hidden />
        </span>
        <div className="min-w-0 flex-1 space-y-0.5">
          <div className="flex items-center gap-2 text-sm font-semibold">
            {t("config.demoTitle")}
            {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}
          </div>
          <p className="text-xs leading-relaxed text-background/70">
            {isRunning ? t("config.demoRunningBanner", { phase: status?.phase ?? "" }) : t("config.demoBody")}
          </p>
        </div>
        <span
          className={cn(
            "shrink-0 rounded-md bg-background px-4 py-2 text-sm font-medium text-foreground",
            "shadow-sm",
          )}
        >
          {isRunning ? t("config.demoInProgress") : t("config.demoDeployShort")}
        </span>
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
              <Rocket className="h-5 w-5" />
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
