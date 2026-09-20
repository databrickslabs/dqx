import { createFileRoute } from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { useTranslation } from "react-i18next";
import { useRef, useState } from "react";
import { toast } from "sonner";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Wrench, Plus, Pencil, Eye, Trash2, Loader2, Upload } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { usePermissions } from "@/hooks/use-permissions";
import selector from "@/lib/selector";
import {
  useListRemediationPlaybookEntries,
  getRemediationPlaybookEntry,
  saveRemediationPlaybookEntry,
  updateRemediationPlaybookContent,
  deleteRemediationPlaybookEntry,
  getListRemediationPlaybookEntriesQueryKey,
  type RemediationPlaybookEntryIn,
  type RemediationPlaybookEntryOut,
} from "@/lib/api";

export const Route = createFileRoute("/_sidebar/remediation-playbook")({
  component: () => <RemediationPlaybookPage />,
});

interface FormState {
  table_fqn: string;
  runbook_yaml: string;
}

const DEFAULT_FORM: FormState = { table_fqn: "", runbook_yaml: "" };

const YAML_PLACEHOLDER = `# Ordered list of remediation rules for this table.
# Shape is up to your re-ingestion pipeline to interpret — the app only
# checks that this parses as YAML, nothing about its contents.

playbooks:
  - rule_name: pacode_not_matching_regex
    priority: 10
    where: "pacode rlike '^[A-Za-z]'"
    strategy: reject_rows
    params:
      reason: "pacode has an alphabetic prefix — not a valid PA code"

  - rule_name: struct_leadid_is_not_unique
    priority: 20
    strategy: dedupe_keep_latest
    params:
      partition_by: [leadId, dispositionreportedon]
      order_by: leaddate
      order_direction: desc
`;

function formatModifiedAt(value: string | null | undefined): string {
  if (!value) return "—";
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString();
}

function RemediationPlaybookPage() {
  const { t } = useTranslation();
  const { canCreateRules, canEditRules } = usePermissions();
  const qc = useQueryClient();

  const {
    data: entries = [],
    isLoading,
    isError,
  } = useListRemediationPlaybookEntries(selector<RemediationPlaybookEntryOut[]>());
  const sorted = [...entries].sort((a, b) => a.filename.localeCompare(b.filename));

  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingFilename, setEditingFilename] = useState<string | null>(null);
  const [form, setForm] = useState<FormState>(DEFAULT_FORM);
  const [loadingEntry, setLoadingEntry] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<RemediationPlaybookEntryOut | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Viewers can open an existing runbook to read it (the GET endpoint
  // already allows all roles) but never edit/save/delete it — the "Add"
  // button and CatalogBrowser table-picker are already gated to
  // canCreateRules, so read-only mode only ever applies when editing an
  // existing entry as a non-editor.
  const readOnly = editingFilename !== null && !canEditRules;

  const update = (patch: Partial<FormState>) => setForm((f) => ({ ...f, ...patch }));

  const saveMutation = useMutation({
    // Editing an existing runbook updates by filename directly (its
    // table_fqn can't be reliably reconstructed for re-derivation, and
    // doesn't need to be — the file's location isn't changing). Only a
    // brand-new runbook (picked via CatalogBrowser) goes through the
    // table_fqn-deriving create path.
    mutationFn: (vars: { filename: string | null; body: RemediationPlaybookEntryIn }) =>
      vars.filename
        ? updateRemediationPlaybookContent(vars.filename, { runbook_yaml: vars.body.runbook_yaml })
        : saveRemediationPlaybookEntry(vars.body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: getListRemediationPlaybookEntriesQueryKey() });
      toast.success(t("remediationPlaybook.entrySaved"));
      closeDialog();
    },
    onError: (err: unknown) => {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? t("remediationPlaybook.failedSave");
      toast.error(detail);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (filename: string) => deleteRemediationPlaybookEntry(filename),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: getListRemediationPlaybookEntriesQueryKey() });
      toast.success(t("remediationPlaybook.entryDeleted"));
      setDeleteTarget(null);
    },
    onError: () => {
      toast.error(t("remediationPlaybook.failedDelete"));
      setDeleteTarget(null);
    },
  });

  const openCreate = () => {
    setEditingFilename(null);
    setForm(DEFAULT_FORM);
    setDialogOpen(true);
  };

  const openEdit = async (e: RemediationPlaybookEntryOut) => {
    setEditingFilename(e.filename);
    setForm({ table_fqn: e.table_fqn, runbook_yaml: "" });
    setDialogOpen(true);
    setLoadingEntry(true);
    try {
      const resp = await getRemediationPlaybookEntry(e.filename);
      setForm({ table_fqn: resp.data.table_fqn, runbook_yaml: resp.data.runbook_yaml ?? "" });
    } catch {
      toast.error(t("remediationPlaybook.failedLoadEntry"));
    } finally {
      setLoadingEntry(false);
    }
  };

  const closeDialog = () => {
    setDialogOpen(false);
    setEditingFilename(null);
    setForm(DEFAULT_FORM);
  };

  const handleFileUpload = (file: File) => {
    const reader = new FileReader();
    reader.onload = () => {
      const text = typeof reader.result === "string" ? reader.result : "";
      update({ runbook_yaml: text });
    };
    reader.onerror = () => toast.error(t("remediationPlaybook.uploadFailed"));
    reader.readAsText(file);
  };

  const handleSave = () => {
    if (!form.table_fqn.trim()) {
      toast.error(t("remediationPlaybook.validationTableRequired"));
      return;
    }
    saveMutation.mutate({
      filename: editingFilename,
      body: { table_fqn: form.table_fqn.trim(), runbook_yaml: form.runbook_yaml },
    });
  };

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-6">
      <PageBreadcrumb page={t("remediationPlaybook.breadcrumb")} />

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">{t("remediationPlaybook.title")}</h1>
          <p className="text-sm text-muted-foreground mt-1">{t("remediationPlaybook.subtitle")}</p>
        </div>
        {canCreateRules && (
          <Button onClick={openCreate} className="gap-2">
            <Plus size={16} />
            {t("remediationPlaybook.addEntry")}
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
          <CardContent className="pt-4 text-sm text-destructive">{t("remediationPlaybook.failedLoad")}</CardContent>
        </Card>
      )}

      {!isLoading && !isError && sorted.length === 0 && (
        <Card>
          <CardContent className="flex flex-col items-center gap-3 py-12 text-center">
            <Wrench size={40} className="text-muted-foreground/50" />
            <div>
              <p className="font-medium">{t("remediationPlaybook.noEntries")}</p>
              <p className="text-sm text-muted-foreground mt-1">{t("remediationPlaybook.noEntriesDescription")}</p>
            </div>
            {canCreateRules && (
              <Button variant="outline" onClick={openCreate} className="gap-2 mt-2">
                <Plus size={16} />
                {t("remediationPlaybook.addEntry")}
              </Button>
            )}
          </CardContent>
        </Card>
      )}

      {sorted.length > 0 && (
        <div className="border rounded-lg overflow-x-auto">
          <table className="w-full text-sm min-w-[700px]">
            <thead className="bg-muted/50 text-muted-foreground text-xs uppercase">
              <tr>
                <th className="text-left font-medium px-3 py-2">{t("remediationPlaybook.colFile")}</th>
                <th className="text-left font-medium px-3 py-2 w-28">{t("remediationPlaybook.colSize")}</th>
                <th className="text-left font-medium px-3 py-2 w-48">{t("remediationPlaybook.colUpdated")}</th>
                <th className="text-right font-medium px-3 py-2 w-28">{t("remediationPlaybook.colActions")}</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((e) => (
                <tr key={e.filename} className="border-t hover:bg-muted/30">
                  <td className="px-3 py-2 font-mono text-xs">{e.filename}</td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {e.size_bytes != null ? `${(e.size_bytes / 1024).toFixed(1)} KB` : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">{formatModifiedAt(e.modified_at)}</td>
                  <td className="px-3 py-2">
                    <div className="flex items-center justify-end gap-1">
                      <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => openEdit(e)}>
                        {canEditRules ? <Pencil size={13} /> : <Eye size={13} />}
                        <span className="sr-only">{canEditRules ? t("remediationPlaybook.edit") : t("remediationPlaybook.view")}</span>
                      </Button>
                      {canEditRules && (
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 text-destructive hover:bg-destructive/10"
                          onClick={() => setDeleteTarget(e)}
                        >
                          <Trash2 size={13} />
                          <span className="sr-only">{t("remediationPlaybook.deleteEntry")}</span>
                        </Button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Create / Edit dialog */}
      <Dialog open={dialogOpen} onOpenChange={(open) => { if (!open) closeDialog(); }}>
        <DialogContent className="sm:max-w-2xl max-h-[85vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {readOnly
                ? t("remediationPlaybook.viewEntryTitle")
                : editingFilename
                  ? t("remediationPlaybook.editEntryTitle")
                  : t("remediationPlaybook.addEntryTitle")}
            </DialogTitle>
            <DialogDescription>
              {readOnly ? t("remediationPlaybook.viewDialogDescription") : t("remediationPlaybook.dialogDescription")}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label>{t("remediationPlaybook.fieldTable")}</Label>
              {editingFilename ? (
                <p className="font-mono text-xs rounded-md border bg-muted px-3 py-2">{form.table_fqn}</p>
              ) : (
                <CatalogBrowser value={form.table_fqn} onChange={(fqn) => update({ table_fqn: fqn })} />
              )}
            </div>

            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <Label htmlFor="pb-yaml">{t("remediationPlaybook.fieldRunbookYaml")}</Label>
                {!readOnly && (
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() => fileInputRef.current?.click()}
                  >
                    <Upload size={12} />
                    {t("remediationPlaybook.uploadYaml")}
                  </Button>
                )}
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".yaml,.yml,text/yaml"
                  className="hidden"
                  onChange={(e) => {
                    const file = e.target.files?.[0];
                    if (file) handleFileUpload(file);
                    e.target.value = "";
                  }}
                />
              </div>
              {loadingEntry ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
                  <Loader2 size={14} className="animate-spin" />
                  {t("common.loading")}
                </div>
              ) : (
                <Textarea
                  id="pb-yaml"
                  rows={16}
                  className="font-mono text-xs"
                  placeholder={YAML_PLACEHOLDER}
                  value={form.runbook_yaml}
                  onChange={(e) => update({ runbook_yaml: e.target.value })}
                  readOnly={readOnly}
                />
              )}
              {!readOnly && <p className="text-xs text-muted-foreground">{t("remediationPlaybook.fieldRunbookYamlHint")}</p>}
            </div>
          </div>

          <DialogFooter>
            {readOnly ? (
              <Button variant="outline" onClick={closeDialog}>
                {t("remediationPlaybook.close")}
              </Button>
            ) : (
              <>
                <Button variant="outline" onClick={closeDialog}>
                  {t("remediationPlaybook.cancel")}
                </Button>
                <Button onClick={handleSave} disabled={!form.table_fqn.trim() || loadingEntry || saveMutation.isPending}>
                  {saveMutation.isPending && <Loader2 size={14} className="animate-spin mr-2" />}
                  {t("remediationPlaybook.save")}
                </Button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation dialog */}
      <Dialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{t("remediationPlaybook.deleteConfirmTitle")}</DialogTitle>
            <DialogDescription>{t("remediationPlaybook.deleteConfirmBody")}</DialogDescription>
          </DialogHeader>
          {deleteTarget && (
            <div className="flex items-center gap-2 rounded-md border p-3 text-sm">
              <Wrench size={14} className="text-primary" />
              <span className="font-mono text-xs font-medium">{deleteTarget.filename}</span>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>
              {t("remediationPlaybook.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={deleteMutation.isPending}
              onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget.filename)}
            >
              {deleteMutation.isPending && <Loader2 size={14} className="animate-spin mr-2" />}
              {t("remediationPlaybook.confirmDelete")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
