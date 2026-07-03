import { createFileRoute } from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { useTranslation } from "react-i18next";
import { useState } from "react";
import { toast } from "sonner";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Bell,
  Plus,
  Pencil,
  Trash2,
  FlaskConical,
  Loader2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Card,
  CardContent,
  CardHeader,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useListAlertChannels,
  createAlertChannel,
  updateAlertChannel,
  deleteAlertChannel,
  testAlertWebhook,
  getAlertChannelsQueryKey,
  type AlertChannelIn,
  type AlertChannelOut,
} from "@/lib/api-custom";

export const Route = createFileRoute("/_sidebar/alerts")({
  component: () => <AlertsPage />,
});

// ---------------------------------------------------------------------------
// Channel form state
// ---------------------------------------------------------------------------

interface ChannelFormState {
  name: string;
  webhook_url: string;
  trigger: "all_runs" | "manual_only" | "scheduled_only";
  enabled: boolean;
  notify_dry_runs: boolean;
}

const DEFAULT_FORM: ChannelFormState = {
  name: "",
  webhook_url: "",
  trigger: "all_runs",
  enabled: true,
  notify_dry_runs: false,
};

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

function AlertsPage() {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const qc = useQueryClient();

  const { data: channels = [], isLoading, isError } = useListAlertChannels();

  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingChannel, setEditingChannel] = useState<AlertChannelOut | null>(null);
  const [form, setForm] = useState<ChannelFormState>(DEFAULT_FORM);
  const [deleteTarget, setDeleteTarget] = useState<AlertChannelOut | null>(null);
  const [testingId, setTestingId] = useState<string | null>(null);

  const saveMutation = useMutation({
    mutationFn: (data: { channelId: string | null; body: AlertChannelIn }) => {
      if (data.channelId) {
        return updateAlertChannel(data.channelId, data.body);
      }
      return createAlertChannel(data.body);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: getAlertChannelsQueryKey() });
      toast.success(t("alerts.channelSaved"));
      setDialogOpen(false);
      setEditingChannel(null);
      setForm(DEFAULT_FORM);
    },
    onError: () => {
      toast.error(t("alerts.failedSave"));
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (channelId: string) => deleteAlertChannel(channelId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: getAlertChannelsQueryKey() });
      toast.success(t("alerts.channelDeleted"));
      setDeleteTarget(null);
    },
    onError: () => {
      toast.error(t("alerts.failedDelete"));
      setDeleteTarget(null);
    },
  });

  const openCreate = () => {
    setEditingChannel(null);
    setForm(DEFAULT_FORM);
    setDialogOpen(true);
  };

  const openEdit = (ch: AlertChannelOut) => {
    setEditingChannel(ch);
    setForm({
      name: ch.name,
      webhook_url: ch.webhook_url,
      trigger: ch.trigger as ChannelFormState["trigger"],
      enabled: ch.enabled,
      notify_dry_runs: ch.notify_dry_runs,
    });
    setDialogOpen(true);
  };

  const handleSave = () => {
    if (!form.name.trim() || !form.webhook_url.trim()) return;
    saveMutation.mutate({
      channelId: editingChannel?.channel_id ?? null,
      body: form,
    });
  };

  const handleTest = async (ch: AlertChannelOut) => {
    setTestingId(ch.channel_id);
    try {
      const resp = await testAlertWebhook(ch.webhook_url);
      if (resp.data.success) {
        toast.success(t("alerts.testWebhookSuccess"));
      } else {
        toast.error(`${t("alerts.testWebhookFailed")}: ${resp.data.message}`);
      }
    } catch {
      toast.error(t("alerts.testWebhookFailed"));
    } finally {
      setTestingId(null);
    }
  };

  const triggerLabel = (trigger: string) => {
    if (trigger === "manual_only") return t("alerts.triggerManualOnly");
    if (trigger === "scheduled_only") return t("alerts.triggerScheduledOnly");
    return t("alerts.triggerAllRuns");
  };

  return (
    <div className="p-6 max-w-4xl mx-auto space-y-6">
      <PageBreadcrumb page={t("alerts.breadcrumb")} />

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">{t("alerts.title")}</h1>
          <p className="text-sm text-muted-foreground mt-1">{t("alerts.subtitle")}</p>
        </div>
        {isAdmin && (
          <Button onClick={openCreate} className="gap-2">
            <Plus size={16} />
            {t("alerts.addChannel")}
          </Button>
        )}
      </div>

      {isLoading && (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 size={16} className="animate-spin" />
          <span>{t("common.loading")}</span>
        </div>
      )}

      {isError && (
        <Card className="border-destructive">
          <CardContent className="pt-4 text-sm text-destructive">{t("alerts.failedLoad")}</CardContent>
        </Card>
      )}

      {!isLoading && !isError && channels.length === 0 && (
        <Card>
          <CardContent className="flex flex-col items-center gap-3 py-12 text-center">
            <Bell size={40} className="text-muted-foreground/50" />
            <div>
              <p className="font-medium">{t("alerts.noChannels")}</p>
              <p className="text-sm text-muted-foreground mt-1">{t("alerts.noChannelsDescription")}</p>
            </div>
            {isAdmin && (
              <Button variant="outline" onClick={openCreate} className="gap-2 mt-2">
                <Plus size={16} />
                {t("alerts.addChannel")}
              </Button>
            )}
          </CardContent>
        </Card>
      )}

      {channels.length > 0 && (
        <div className="space-y-3">
          {channels.map((ch) => (
            <Card key={ch.channel_id} className="overflow-hidden">
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-2 min-w-0">
                    <Bell size={16} className="shrink-0 text-primary" />
                    <span className="font-semibold text-base truncate">{ch.name}</span>
                    <Badge variant={ch.enabled ? "default" : "secondary"} className="shrink-0 text-xs">
                      {ch.enabled ? t("alerts.enabled") : t("alerts.disabled")}
                    </Badge>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-1"
                      disabled={testingId === ch.channel_id}
                      onClick={() => handleTest(ch)}
                    >
                      {testingId === ch.channel_id ? (
                        <Loader2 size={13} className="animate-spin" />
                      ) : (
                        <FlaskConical size={13} />
                      )}
                      {t("alerts.testWebhook")}
                    </Button>
                    {isAdmin && (
                      <>
                        <Button variant="outline" size="sm" onClick={() => openEdit(ch)}>
                          <Pencil size={13} />
                          <span className="sr-only">{t("alerts.edit")}</span>
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          className="text-destructive hover:bg-destructive/10"
                          onClick={() => setDeleteTarget(ch)}
                        >
                          <Trash2 size={13} />
                          <span className="sr-only">{t("alerts.deleteChannel")}</span>
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              </CardHeader>
              <CardContent className="pt-0 space-y-2">
                <p
                  className="font-mono text-xs text-muted-foreground bg-muted rounded px-2 py-1 overflow-hidden text-ellipsis whitespace-nowrap"
                  title={ch.webhook_url}
                >
                  {ch.webhook_url}
                </p>
                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
                  <span>
                    <span className="font-medium text-foreground">{t("alerts.trigger")}:</span>{" "}
                    {triggerLabel(ch.trigger)}
                  </span>
                  {ch.notify_dry_runs && (
                    <span className="font-medium text-foreground">{t("alerts.notifyDryRunsOn")}</span>
                  )}
                  <span className="text-foreground font-medium">{t("alerts.teamsWebhook")}</span>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Create / Edit dialog */}
      <Dialog open={dialogOpen} onOpenChange={(open) => { if (!open) { setDialogOpen(false); setEditingChannel(null); setForm(DEFAULT_FORM); } }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>
              {editingChannel ? t("alerts.editChannelTitle") : t("alerts.addChannelTitle")}
            </DialogTitle>
            <DialogDescription>{t("alerts.teamsWebhook")}</DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label htmlFor="alert-channel-name">{t("alerts.channelName")}</Label>
              <Input
                id="alert-channel-name"
                placeholder={t("alerts.channelNamePlaceholder")}
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              />
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="alert-webhook-url">{t("alerts.webhookUrl")}</Label>
              <Input
                id="alert-webhook-url"
                placeholder={t("alerts.webhookUrlPlaceholder")}
                value={form.webhook_url}
                onChange={(e) => setForm((f) => ({ ...f, webhook_url: e.target.value }))}
              />
            </div>

            <div className="space-y-1.5">
              <Label>{t("alerts.trigger")}</Label>
              <Select
                value={form.trigger}
                onValueChange={(v) => setForm((f) => ({ ...f, trigger: v as ChannelFormState["trigger"] }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all_runs">{t("alerts.triggerAllRuns")}</SelectItem>
                  <SelectItem value="manual_only">{t("alerts.triggerManualOnly")}</SelectItem>
                  <SelectItem value="scheduled_only">{t("alerts.triggerScheduledOnly")}</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center gap-3">
              <Switch
                id="alert-enabled"
                checked={form.enabled}
                onCheckedChange={(checked) => setForm((f) => ({ ...f, enabled: checked }))}
              />
              <Label htmlFor="alert-enabled">{t("alerts.enabled")}</Label>
            </div>

            <div className="flex items-center gap-3">
              <Switch
                id="alert-dry-runs"
                checked={form.notify_dry_runs}
                onCheckedChange={(checked) => setForm((f) => ({ ...f, notify_dry_runs: checked }))}
              />
              <Label htmlFor="alert-dry-runs">{t("alerts.notifyDryRuns")}</Label>
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => { setDialogOpen(false); setEditingChannel(null); setForm(DEFAULT_FORM); }}>
              {t("alerts.cancel")}
            </Button>
            <Button
              onClick={handleSave}
              disabled={!form.name.trim() || !form.webhook_url.trim() || saveMutation.isPending}
            >
              {saveMutation.isPending && <Loader2 size={14} className="animate-spin mr-2" />}
              {t("alerts.save")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation dialog */}
      <Dialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{t("alerts.deleteConfirmTitle")}</DialogTitle>
            <DialogDescription>{t("alerts.deleteConfirmBody")}</DialogDescription>
          </DialogHeader>
          {deleteTarget && (
            <div className="flex items-center gap-2 rounded-md border p-3 text-sm">
              <Bell size={14} className="text-primary" />
              <span className="font-medium">{deleteTarget.name}</span>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>
              {t("alerts.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={deleteMutation.isPending}
              onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget.channel_id)}
            >
              {deleteMutation.isPending && <Loader2 size={14} className="animate-spin mr-2" />}
              {t("alerts.confirmDelete")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
