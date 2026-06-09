import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { AlertCircle, Clock, Globe, Loader2, Search, Tags, Plus, Trash2, X } from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";
import { RoleManagement } from "@/components/RoleManagement";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import {
  useTimezone,
  useSaveTimezone,
  getTimezoneQueryKey,
  useLabelDefinitions,
  useSaveLabelDefinitions,
  getLabelDefinitionsQueryKey,
  useRetentionSettings,
  useSaveRetentionSettings,
  getRetentionSettingsQueryKey,
  type LabelDefinition,
  type RetentionSettingsOut,
} from "@/lib/api-custom";
import type { AxiosError } from "axios";
import { toast } from "sonner";
import { useCurrentUserRoleSuspense } from "@/hooks/use-suspense-queries";
import { usePermissions } from "@/hooks/use-permissions";
import { Suspense, useMemo, useState, useRef, useEffect } from "react";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { ChevronDown, Check } from "lucide-react";

export const Route = createFileRoute("/_sidebar/config")({
  component: () => <ConfigPage />,
});

function getAllTimezones(): { value: string; label: string; offset: string }[] {
  let zones: string[];
  try {
    zones = (Intl as unknown as { supportedValuesOf(k: string): string[] }).supportedValuesOf("timeZone");
  } catch {
    zones = [];
  }
  if (!zones.includes("UTC")) zones = ["UTC", ...zones];

  const now = new Date();
  return zones.map((tz) => {
    const short = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      timeZoneName: "short",
    })
      .formatToParts(now)
      .find((p) => p.type === "timeZoneName")?.value ?? "";
    const offset = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      timeZoneName: "longOffset",
    })
      .formatToParts(now)
      .find((p) => p.type === "timeZoneName")?.value ?? "";
    const city = tz.split("/").pop()?.replace(/_/g, " ") ?? tz;
    return {
      value: tz,
      label: `${tz} (${short})`,
      offset,
      _city: city,
      _region: tz.split("/")[0] ?? "",
    };
  });
}

const ALL_TIMEZONES = getAllTimezones();

function SectionError({
  resetErrorBoundary,
}: {
  resetErrorBoundary: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-2 items-start">
      <p className="text-sm text-destructive flex items-center gap-1">
        <AlertCircle className="h-4 w-4" /> {t("config.sectionLoadFailed")}
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
        {t("common.retry")}
      </Button>
    </div>
  );
}

function TimezoneSettings() {
  const { t } = useTranslation();
  const { data: tz, isLoading } = useTimezone();
  const saveMutation = useSaveTimezone();
  const queryClient = useQueryClient();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  const currentTz = (tz as { timezone: string } | undefined)?.timezone ?? "UTC";

  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 50);
  }, [open]);

  const filtered = useMemo(() => {
    if (!search) return ALL_TIMEZONES;
    const q = search.toLowerCase();
    return ALL_TIMEZONES.filter(
      (tz) =>
        tz.value.toLowerCase().includes(q) ||
        tz.label.toLowerCase().includes(q) ||
        tz.offset.toLowerCase().includes(q),
    );
  }, [search]);

  const handleSelect = (value: string) => {
    setOpen(false);
    setSearch("");
    if (value === currentTz) return;
    saveMutation.mutate(
      { data: { timezone: value } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getTimezoneQueryKey() });
          toast.success(t("config.timezoneUpdated", { value }));
        },
        onError: () => toast.error(t("config.failedToSaveTimezone")),
      },
    );
  };

  if (isLoading) return <Skeleton className="h-10 w-64" />;

  const currentLabel = ALL_TIMEZONES.find((t) => t.value === currentTz)?.label ?? currentTz;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Globe className="h-5 w-5" />
          {t("config.timezoneTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-3">
          <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                role="combobox"
                aria-expanded={open}
                className="w-96 justify-between font-normal"
                disabled={!isAdmin || saveMutation.isPending}
              >
                <span className="truncate">{currentLabel}</span>
                <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-96 p-0" align="start">
              <div className="flex items-center border-b px-3 py-2">
                <Search className="mr-2 h-4 w-4 shrink-0 opacity-50" />
                <Input
                  ref={inputRef}
                  placeholder={t("config.searchTimezones")}
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="h-8 border-0 p-0 shadow-none focus-visible:ring-0"
                />
              </div>
              <div className="max-h-72 overflow-y-auto">
                {filtered.length === 0 && (
                  <p className="px-3 py-4 text-sm text-muted-foreground text-center">{t("config.noTimezoneFound")}</p>
                )}
                {filtered.map((opt) => (
                  <button
                    key={opt.value}
                    type="button"
                    className={cn(
                      "flex w-full items-center gap-2 px-3 py-2 text-sm hover:bg-accent hover:text-accent-foreground cursor-pointer",
                      opt.value === currentTz && "bg-accent/50 font-medium",
                    )}
                    onClick={() => handleSelect(opt.value)}
                  >
                    <Check className={cn("h-4 w-4 shrink-0", opt.value === currentTz ? "opacity-100 text-green-500" : "opacity-0")} />
                    <span className="truncate">{opt.label}</span>
                    <span className="ml-auto text-xs text-muted-foreground shrink-0">{opt.offset}</span>
                  </button>
                ))}
              </div>
            </PopoverContent>
          </Popover>
          {saveMutation.isPending && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
          {!isAdmin && (
            <span className="text-xs text-muted-foreground">{t("config.adminOnly")}</span>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Label Definitions — admin-managed catalog of label keys + allowed values.
// Drives the constrained label picker on rule authoring pages, including the
// reserved ``weight`` key (which controls the weight selector / row).
// ─────────────────────────────────────────────────────────────────────────────

const LABEL_KEY_RE = /^[A-Za-z][A-Za-z0-9_]*$/;
const RESERVED_WEIGHT_KEY = "weight";

interface DraftDefinition extends LabelDefinition {
  draftId: string;
  newValueDraft: string;
}

function defToDraft(d: LabelDefinition): DraftDefinition {
  return {
    draftId: crypto.randomUUID(),
    key: d.key,
    description: d.description ?? "",
    values: [...d.values],
    allow_custom_values: !!d.allow_custom_values,
    newValueDraft: "",
  };
}

function draftToDef(d: DraftDefinition): LabelDefinition {
  return {
    key: d.key.trim(),
    description: (d.description ?? "").trim(),
    values: d.values.map((v) => v.trim()).filter(Boolean),
    allow_custom_values: d.allow_custom_values,
  };
}

function LabelDefinitionsSettings() {
  const { t } = useTranslation();
  const { data, isLoading } = useLabelDefinitions();
  const queryClient = useQueryClient();
  const saveMutation = useSaveLabelDefinitions();

  const [drafts, setDrafts] = useState<DraftDefinition[]>([]);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (data && !hydrated) {
      setDrafts((data.definitions ?? []).map(defToDraft));
      setHydrated(true);
    }
  }, [data, hydrated]);

  const isDirty = useMemo(() => {
    if (!data) return false;
    const a = data.definitions ?? [];
    const b = drafts.map(draftToDef);
    if (a.length !== b.length) return true;
    return JSON.stringify(a) !== JSON.stringify(b);
  }, [data, drafts]);

  const validation = useMemo(() => {
    const errors: string[] = [];
    const seen = new Set<string>();
    for (const d of drafts) {
      const k = d.key.trim();
      if (!k) {
        errors.push(t("config.labelKeyMissing"));
        continue;
      }
      if (!LABEL_KEY_RE.test(k)) {
        errors.push(t("config.labelKeyInvalid", { key: k }));
      }
      if (seen.has(k)) errors.push(t("config.labelKeyDuplicate", { key: k }));
      seen.add(k);
    }
    return errors;
  }, [drafts, t]);

  const updateDraft = (draftId: string, patch: Partial<DraftDefinition>) => {
    setDrafts((prev) => prev.map((d) => (d.draftId === draftId ? { ...d, ...patch } : d)));
  };

  const removeDraft = (draftId: string) =>
    setDrafts((prev) => prev.filter((d) => d.draftId !== draftId));

  const addDraft = (initialKey?: string) =>
    setDrafts((prev) => [
      ...prev,
      {
        draftId: crypto.randomUUID(),
        key: initialKey ?? "",
        description: "",
        values: [],
        allow_custom_values: false,
        newValueDraft: "",
      },
    ]);

  const addValue = (draftId: string) => {
    const target = drafts.find((d) => d.draftId === draftId);
    if (!target) return;
    const v = target.newValueDraft.trim();
    if (!v) return;
    if (target.values.includes(v)) {
      updateDraft(draftId, { newValueDraft: "" });
      return;
    }
    updateDraft(draftId, {
      values: [...target.values, v],
      newValueDraft: "",
    });
  };

  const removeValue = (draftId: string, value: string) => {
    const target = drafts.find((d) => d.draftId === draftId);
    if (!target) return;
    updateDraft(draftId, { values: target.values.filter((v) => v !== value) });
  };

  const handleSave = () => {
    if (validation.length > 0) {
      toast.error(validation[0]);
      return;
    }
    const definitions = drafts.map(draftToDef);
    saveMutation.mutate(
      { data: { definitions } },
      {
        onSuccess: (resp) => {
          queryClient.invalidateQueries({ queryKey: getLabelDefinitionsQueryKey() });
          setDrafts(resp.data.definitions.map(defToDraft));
          toast.success(
            definitions.length === 0
              ? t("config.clearedLabels")
              : t("config.savedLabels", { count: definitions.length }),
          );
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.failedSaveLabels"));
        },
      },
    );
  };

  const handleReset = () => {
    setDrafts((data?.definitions ?? []).map(defToDraft));
  };

  const hasWeightKey = drafts.some((d) => d.key.trim() === RESERVED_WEIGHT_KEY);

  if (isLoading) {
    return <Skeleton className="h-40 w-full" />;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Tags className="h-5 w-5" />
          {t("config.labelsTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {drafts.length === 0 && (
          <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
            {t("config.noLabelDefinitions")}
          </div>
        )}
        {drafts.map((d) => (
          <DefinitionEditorCard
            key={d.draftId}
            draft={d}
            onChange={(patch) => updateDraft(d.draftId, patch)}
            onRemove={() => removeDraft(d.draftId)}
            onAddValue={() => addValue(d.draftId)}
            onRemoveValue={(v) => removeValue(d.draftId, v)}
          />
        ))}
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => addDraft()} className="gap-1.5">
            <Plus className="h-3.5 w-3.5" />
            {t("config.addLabelDefinition")}
          </Button>
          {!hasWeightKey && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => addDraft(RESERVED_WEIGHT_KEY)}
              className="gap-1.5 text-xs text-muted-foreground"
              title={t("config.addWeightTooltip")}
            >
              <Plus className="h-3.5 w-3.5" />
              {t("config.addWeightDefinition")}
            </Button>
          )}
        </div>
        {validation.length > 0 && (
          <div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 space-y-1">
            {validation.map((msg, i) => (
              <p key={i} className="text-xs text-destructive flex items-start gap-1.5">
                <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                <span>{msg}</span>
              </p>
            ))}
          </div>
        )}
        <div className="flex items-center gap-2 pt-2 border-t">
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!isDirty || validation.length > 0 || saveMutation.isPending}
          >
            {saveMutation.isPending && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            {t("config.saveChanges")}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={handleReset}
            disabled={!isDirty || saveMutation.isPending}
          >
            {t("config.reset")}
          </Button>
          {!isDirty && (data?.definitions?.length ?? 0) > 0 && (
            <span className="text-xs text-muted-foreground">
              {t("config.definitionsActive", { count: data?.definitions?.length ?? 0 })}
            </span>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

interface DefinitionEditorCardProps {
  draft: DraftDefinition;
  onChange: (patch: Partial<DraftDefinition>) => void;
  onRemove: () => void;
  onAddValue: () => void;
  onRemoveValue: (value: string) => void;
}

function DefinitionEditorCard({
  draft,
  onChange,
  onRemove,
  onAddValue,
  onRemoveValue,
}: DefinitionEditorCardProps) {
  const { t } = useTranslation();
  const keyValid = !draft.key || LABEL_KEY_RE.test(draft.key.trim());
  const isWeight = draft.key.trim() === RESERVED_WEIGHT_KEY;
  return (
    <div className={cn("rounded-md border p-3 space-y-3", isWeight ? "bg-blue-50/30 border-blue-200/60" : "bg-muted/20")}>
      <div className="flex items-start gap-3">
        <div className="grid grid-cols-[180px_1fr] gap-3 flex-1 items-start">
          <div className="space-y-1">
            <Label className="text-xs flex items-center gap-1.5">
              {t("config.key")}
              {isWeight && (
                <Badge variant="secondary" className="h-4 px-1 text-[10px] font-normal">
                  {t("config.reserved")}
                </Badge>
              )}
            </Label>
            <Input
              value={draft.key}
              onChange={(e) => onChange({ key: e.target.value })}
              placeholder={t("config.keyPlaceholder")}
              className={cn("h-8 text-xs font-mono", !keyValid && "border-destructive")}
            />
            {!keyValid && (
              <p className="text-[10px] text-destructive">
                {t("config.keyHint")}
              </p>
            )}
            {isWeight && (
              <p className="text-[10px] text-blue-700">
                {t("config.weightHint")}
              </p>
            )}
          </div>
          <div className="space-y-1">
            <Label className="text-xs">
              {t("config.descriptionLabel")} <span className="text-muted-foreground">{t("config.optional")}</span>
            </Label>
            <Textarea
              value={draft.description ?? ""}
              onChange={(e) => onChange({ description: e.target.value })}
              placeholder={isWeight ? t("config.weightDescriptionPlaceholder") : t("config.descriptionPlaceholder")}
              className="text-xs min-h-[32px] py-1.5"
              rows={1}
            />
          </div>
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-8 w-8 shrink-0 text-destructive hover:text-destructive"
          onClick={onRemove}
          aria-label={t("config.removeDefinition")}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
      <div className="space-y-1.5">
        <div className="flex items-center justify-between">
          <Label className="text-xs">
            {t("config.allowedValues")}{" "}
            <span className="text-muted-foreground">
              {t("config.allowedValuesHint")}
            </span>
          </Label>
          <label className="flex items-center gap-1.5 text-xs cursor-pointer">
            <Checkbox
              checked={draft.allow_custom_values}
              onCheckedChange={(c) => onChange({ allow_custom_values: c === true })}
            />
            <span>{t("config.allowCustomValues")}</span>
          </label>
        </div>
        {draft.values.length > 0 ? (
          <div className="flex flex-wrap gap-1.5">
            {draft.values.map((v) => (
              <Badge
                key={v}
                variant="secondary"
                className="h-6 gap-1 pl-2 pr-1 text-xs font-normal"
              >
                <span className="font-mono">{v}</span>
                <button
                  type="button"
                  className="ml-0.5 rounded-full hover:bg-foreground/10 p-0.5"
                  onClick={() => onRemoveValue(v)}
                  aria-label={t("config.removeValueAria", { value: v })}
                >
                  <X className="h-3 w-3" />
                </button>
              </Badge>
            ))}
          </div>
        ) : (
          <p className="text-[11px] italic text-muted-foreground">
            {t("config.noValuesHint")}
          </p>
        )}
        <div className="flex items-center gap-1.5">
          <Input
            value={draft.newValueDraft}
            onChange={(e) => onChange({ newValueDraft: e.target.value })}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                onAddValue();
              }
            }}
            placeholder={isWeight ? t("config.weightValuePlaceholder") : t("config.addValuePlaceholder")}
            className="h-7 text-xs flex-1 font-mono"
          />
          <Button
            type="button"
            size="sm"
            variant="outline"
            className="h-7 text-xs gap-1"
            disabled={!draft.newValueDraft.trim()}
            onClick={onAddValue}
          >
            <Plus className="h-3 w-3" />
            {t("common.add")}
          </Button>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Retention Settings — admin-controlled DELETE windows for the daily sweep.
// Two knobs: a global retention applied to dq_validation_runs, dq_metrics,
// dq_profiling_results + the OLTP history tables; and a tighter
// quarantine-specific retention applied only to dq_quarantine_records (which
// holds full source row payloads + errors/warnings). The split exists so PII
// can age out faster than trend tables that the dashboards look back on.
// ─────────────────────────────────────────────────────────────────────────────

function RetentionSettings() {
  const { data, isLoading } = useRetentionSettings();
  const queryClient = useQueryClient();
  const saveMutation = useSaveRetentionSettings();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  const settings = data as RetentionSettingsOut | undefined;
  const [global, setGlobal] = useState<string>("");
  const [quarantine, setQuarantine] = useState<string>("");
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (settings && !hydrated) {
      setGlobal(String(settings.retention_days));
      setQuarantine(String(settings.quarantine_retention_days));
      setHydrated(true);
    }
  }, [settings, hydrated]);

  const min = settings?.retention_days_min ?? 7;
  const max = settings?.retention_days_max ?? 3650;

  const parsedGlobal = Number.parseInt(global, 10);
  const parsedQuarantine = Number.parseInt(quarantine, 10);

  const validation = useMemo(() => {
    const errors: string[] = [];
    const check = (label: string, value: number) => {
      if (Number.isNaN(value)) {
        errors.push(`${label} must be a whole number of days.`);
        return;
      }
      if (value < min) errors.push(`${label} must be at least ${min} days.`);
      if (value > max) errors.push(`${label} must be at most ${max} days.`);
    };
    check("Global retention", parsedGlobal);
    check("Quarantine retention", parsedQuarantine);
    return errors;
  }, [parsedGlobal, parsedQuarantine, min, max]);

  const isDirty = useMemo(() => {
    if (!settings) return false;
    return (
      parsedGlobal !== settings.retention_days ||
      parsedQuarantine !== settings.quarantine_retention_days
    );
  }, [settings, parsedGlobal, parsedQuarantine]);

  const handleSave = () => {
    if (!settings || validation.length > 0) return;
    const payload: { retention_days?: number; quarantine_retention_days?: number } = {};
    if (parsedGlobal !== settings.retention_days) payload.retention_days = parsedGlobal;
    if (parsedQuarantine !== settings.quarantine_retention_days) {
      payload.quarantine_retention_days = parsedQuarantine;
    }
    saveMutation.mutate(
      { data: payload },
      {
        onSuccess: (resp) => {
          queryClient.invalidateQueries({ queryKey: getRetentionSettingsQueryKey() });
          setGlobal(String(resp.data.retention_days));
          setQuarantine(String(resp.data.quarantine_retention_days));
          toast.success("Retention settings saved.");
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? "Failed to save retention settings.");
        },
      },
    );
  };

  const handleReset = () => {
    if (!settings) return;
    setGlobal(String(settings.retention_days));
    setQuarantine(String(settings.quarantine_retention_days));
  };

  const resetToDefaults = () => {
    if (!settings) return;
    setGlobal(String(settings.retention_days_default));
    setQuarantine(String(settings.quarantine_retention_days_default));
  };

  if (isLoading || !settings) return <Skeleton className="h-40 w-full" />;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Clock className="h-5 w-5" />
          Data Retention
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          The scheduler runs a daily DELETE pass against the analytical tables.
          <strong className="text-foreground"> Quarantine</strong> holds the full source
          row payload (errors, warnings, and the row itself) so its window is kept
          tighter than the trend tables by default. Both values are floored at{" "}
          <code>{min}</code> days to protect against accidental data loss.
        </p>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="space-y-1.5">
            <Label htmlFor="retention-global" className="text-xs">
              Global retention (days)
            </Label>
            <Input
              id="retention-global"
              type="number"
              min={min}
              max={max}
              step={1}
              value={global}
              disabled={!isAdmin || saveMutation.isPending}
              onChange={(e) => setGlobal(e.target.value)}
              className="h-8"
            />
            <p className="text-[11px] text-muted-foreground">
              Applies to <code>dq_validation_runs</code>, <code>dq_profiling_results</code>,{" "}
              <code>dq_metrics</code>, and the OLTP history tables.
              <br />
              Default: <code>{settings.retention_days_default}</code> days
              {!settings.retention_days_set && " (not yet customised)"}
            </p>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="retention-quarantine" className="text-xs">
              Quarantine retention (days)
            </Label>
            <Input
              id="retention-quarantine"
              type="number"
              min={min}
              max={max}
              step={1}
              value={quarantine}
              disabled={!isAdmin || saveMutation.isPending}
              onChange={(e) => setQuarantine(e.target.value)}
              className="h-8"
            />
            <p className="text-[11px] text-muted-foreground">
              Applies only to <code>dq_quarantine_records</code> (the table that
              stores per-row failures, including the source row payload).
              <br />
              Default: <code>{settings.quarantine_retention_days_default}</code> days
              {!settings.quarantine_retention_days_set && " (not yet customised)"}
            </p>
          </div>
        </div>

        {validation.length > 0 && (
          <div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 space-y-1">
            {validation.map((msg, i) => (
              <p key={i} className="text-xs text-destructive flex items-start gap-1.5">
                <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                <span>{msg}</span>
              </p>
            ))}
          </div>
        )}

        <div className="flex items-center gap-2 pt-2 border-t">
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!isAdmin || !isDirty || validation.length > 0 || saveMutation.isPending}
          >
            {saveMutation.isPending && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            Save changes
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={handleReset}
            disabled={!isAdmin || !isDirty || saveMutation.isPending}
          >
            Reset
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={resetToDefaults}
            disabled={!isAdmin || saveMutation.isPending}
            title="Restore both fields to the system defaults (does not save until you click Save changes)"
          >
            Restore defaults
          </Button>
          {!isAdmin && (
            <span className="text-xs text-muted-foreground">
              Only admins can change retention.
            </span>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function ConfigPage() {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const navigate = useNavigate();

  useEffect(() => {
    if (!isAdmin) {
      navigate({ to: "/rules/active", replace: true });
    }
  }, [isAdmin, navigate]);

  if (!isAdmin) {
    return null;
  }

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb page={t("config.breadcrumb")} />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text={t("config.title")} speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            {t("config.subtitle")}
          </p>
        </div>
      </div>

      <QueryErrorResetBoundary>
        {({ reset }) => (
          <div className="space-y-6 pb-8">
            <FadeIn delay={0.05}>
              <ErrorBoundary onReset={reset} fallbackRender={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <TimezoneSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.1}>
              <ErrorBoundary onReset={reset} fallbackRender={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <LabelDefinitionsSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.15}>
              <ErrorBoundary onReset={reset} fallbackRender={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <RetentionSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.2}>
              <ErrorBoundary onReset={reset} fallbackRender={SectionError}>
                <RoleManagement />
              </ErrorBoundary>
            </FadeIn>
          </div>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}
