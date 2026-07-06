import { createFileRoute } from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { useTranslation } from "react-i18next";
import { useState, useMemo } from "react";
import { toast } from "sonner";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Bell,
  Plus,
  Pencil,
  Trash2,
  FlaskConical,
  Loader2,
  ChevronsUpDown,
  X,
  Copy,
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
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { usePermissions } from "@/hooks/use-permissions";
import { useListRules } from "@/lib/api";
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
  scope_mode: "all" | "tables";
  scope_tables: string[];
}

const DEFAULT_FORM: ChannelFormState = {
  name: "",
  webhook_url: "",
  trigger: "all_runs",
  enabled: true,
  notify_dry_runs: false,
  scope_mode: "all",
  scope_tables: [],
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

  // Fetch approved rules to populate the table scope selector
  const { data: approvedRulesResp } = useListRules({ status: "approved" }, { query: { enabled: dialogOpen } });
  const approvedTableFqns = useMemo(() => {
    const rules = approvedRulesResp?.data ?? [];
    const fqns = new Set(rules.map((r) => r.table_fqn).filter(Boolean));
    return Array.from(fqns).sort() as string[];
  }, [approvedRulesResp]);

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

  const handleCopy = async (value: string, message: string) => {
    try {
      await navigator.clipboard.writeText(value);
      toast.success(message, { duration: 1500 });
    } catch {
      toast.error(t("alerts.couldNotCopy"));
    }
  };

  // Site24x7 (or any uptime monitor) polls these directly, authenticating
  // with a Databricks OAuth token — not scoped to any single channel.
  const monitoringEndpoints = useMemo(() => {
    if (typeof window === "undefined") return [];
    const origin = window.location.origin;
    const paths = [
      { label: t("alerts.endpointTableLabel"), path: "/api/v1/alerts/status/table/{table_fqn}" },
      { label: t("alerts.endpointRunLabel"), path: "/api/v1/alerts/status/run/{run_id}" },
    ];
    return paths.map((p) => ({ ...p, url: `${origin}${p.path}` }));
  }, [t]);

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
      scope_mode: (ch.scope_mode as ChannelFormState["scope_mode"]) ?? "all",
      scope_tables: ch.scope_tables ?? [],
    });
    setDialogOpen(true);
  };

  const handleSave = () => {
    if (!form.name.trim() || !form.webhook_url.trim()) return;
    const body: AlertChannelIn = {
      ...form,
      scope_tables: form.scope_mode === "all" ? [] : form.scope_tables,
    };
    saveMutation.mutate({
      channelId: editingChannel?.channel_id ?? null,
      body,
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

  const toggleScopeTable = (fqn: string) => {
    setForm((f) => {
      const already = f.scope_tables.includes(fqn);
      return {
        ...f,
        scope_tables: already
          ? f.scope_tables.filter((t) => t !== fqn)
          : [...f.scope_tables, fqn],
      };
    });
  };

  const closeDialog = () => {
    setDialogOpen(false);
    setEditingChannel(null);
    setForm(DEFAULT_FORM);
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
                  {ch.scope_mode === "tables" && ch.scope_tables?.length > 0 && (
                    <span>
                      <span className="font-medium text-foreground">{t("alerts.scopeTables")}:</span>{" "}
                      {ch.scope_tables.length === 1
                        ? ch.scope_tables[0].split(".").pop()
                        : t("alerts.scopeTableCount", { count: ch.scope_tables.length })}
                    </span>
                  )}
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

      <div className="space-y-2 rounded-md border p-4">
        <div>
          <h2 className="text-base font-semibold">{t("alerts.monitoringEndpointsTitle")}</h2>
          <p className="text-xs text-muted-foreground mt-0.5">{t("alerts.monitoringEndpointsDescription")}</p>
        </div>
        {monitoringEndpoints.map((row) => (
          <div key={row.url} className="flex items-center gap-2">
            <Badge variant="secondary" className="shrink-0 font-mono text-[10px]">
              GET
            </Badge>
            <code className="min-w-0 flex-1 truncate rounded bg-muted px-2 py-1 text-xs font-mono" title={row.url}>
              {row.path}
            </code>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              onClick={() => handleCopy(row.url, t("alerts.endpointCopied"))}
              aria-label={t("alerts.copy")}
            >
              <Copy size={12} />
            </Button>
          </div>
        ))}
      </div>

      {/* Create / Edit dialog */}
      <Dialog open={dialogOpen} onOpenChange={(open) => { if (!open) closeDialog(); }}>
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

            {/* Scope selection */}
            <div className="space-y-1.5">
              <Label>{t("alerts.scopeLabel")}</Label>
              <Select
                value={form.scope_mode}
                onValueChange={(v) =>
                  setForm((f) => ({
                    ...f,
                    scope_mode: v as ChannelFormState["scope_mode"],
                    scope_tables: v === "all" ? [] : f.scope_tables,
                  }))
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t("alerts.scopeAll")}</SelectItem>
                  <SelectItem value="tables">{t("alerts.scopeSpecific")}</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {form.scope_mode === "tables" && (
              <div className="space-y-1.5">
                <Label>{t("alerts.scopeSelectTables")}</Label>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button variant="outline" className="w-full justify-between font-normal">
                      {form.scope_tables.length === 0
                        ? t("alerts.scopeSelectTablesPlaceholder")
                        : t("alerts.scopeTableCount", { count: form.scope_tables.length })}
                      <ChevronsUpDown size={14} className="opacity-60" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-[var(--radix-popover-trigger-width)] max-h-56 overflow-y-auto p-2" align="start">
                    {approvedTableFqns.length === 0 ? (
                      <p className="px-2 py-1 text-sm text-muted-foreground">{t("alerts.noApprovedTables")}</p>
                    ) : (
                      approvedTableFqns.map((fqn) => (
                        <label
                          key={fqn}
                          className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-accent cursor-pointer text-sm"
                        >
                          <Checkbox
                            checked={form.scope_tables.includes(fqn)}
                            onCheckedChange={() => toggleScopeTable(fqn)}
                          />
                          <span className="truncate">{fqn}</span>
                        </label>
                      ))
                    )}
                  </PopoverContent>
                </Popover>

                {form.scope_tables.length > 0 && (
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {form.scope_tables.map((fqn) => (
                      <Badge key={fqn} variant="secondary" className="gap-1 max-w-xs">
                        <span className="truncate text-xs">{fqn.split(".").pop()}</span>
                        <button
                          type="button"
                          onClick={() => toggleScopeTable(fqn)}
                          className="shrink-0 opacity-60 hover:opacity-100"
                          aria-label={`Remove ${fqn}`}
                        >
                          <X size={10} />
                        </button>
                      </Badge>
                    ))}
                  </div>
                )}
              </div>
            )}

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
            <Button variant="outline" onClick={closeDialog}>
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
