import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { AlertCircle, AlertTriangle, CheckCircle2, Clock, Cpu, Database, ExternalLink, FlaskConical, Globe, KeyRound, LineChart, Loader2, Lock, Scale, Search, SlidersHorizontal, Tags, Plus, Trash2, X, ShieldCheck, Sparkles } from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";
import { RoleManagement } from "@/components/RoleManagement";
import { SampleSelector, type SampleKind } from "@/components/rules/test/RuleTestPanel";
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
import { Switch } from "@/components/ui/switch";
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
  useRunReviewStatuses,
  useSaveRunReviewStatuses,
  getRunReviewStatusesQueryKey,
  type RetentionSettingsOut,
  type RunReviewStatusOption,
  useWorkspaceHost,
} from "@/lib/api-custom";
import {
  HEX_COLOR_RE,
  defToDraft,
  draftToDef,
  type CriticalityValue,
  type DraftDefinition,
} from "@/lib/label-definition-drafts";
import { resolveCriticality } from "@/lib/registry-rule-conversion";
import {
  useGetAiSettings,
  useSaveAiSettings,
  getGetAiSettingsQueryKey,
  useListServingEndpoints,
  useEnsureVectorStore,
  useGetRulesRegistrySettings,
  useSaveRulesRegistrySettings,
  getGetRulesRegistrySettingsQueryKey,
  useGetApprovalsMode,
  useSaveApprovalsMode,
  getGetApprovalsModeQueryKey,
  useGetComputeSettingsSuspense,
  useSaveComputeSettings,
  getGetComputeSettingsQueryKey,
  useGetPermissionsDefaultInherit,
  useSetPermissionsDefaultInherit,
  getGetPermissionsDefaultInheritQueryKey,
  useGetGlobalResultsSettings,
  useSaveGlobalResultsSettings,
  getGetGlobalResultsSettingsQueryKey,
  useGetRequireDraftRunSettings,
  useSaveRequireDraftRunSettings,
  getGetRequireDraftRunSettingsQueryKey,
  useListComputeWarehousesSuspense,
  useListComputeClustersSuspense,
  useGetWarehouseAccess,
  getGetWarehouseAccessQueryKey,
  useGrantWarehouseAccess,
  useGetDraftRunSampleLimit,
  useSaveDraftRunSampleLimit,
  getGetDraftRunSampleLimitQueryKey,
  useDeployDemoContent,
  useDemoContentStatus,
  getDemoContentStatusQueryKey,
  type AiSettingsIn,
  type RulesRegistrySettingsIn,
  type ComputeSettingsIn,
  type JobsComputeModel,
  type WarehouseOut,
  type ClusterOut,
} from "@/lib/api";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useResetDatabase } from "@/lib/api";
import type { AxiosError } from "axios";
import { toast } from "sonner";
import { useCurrentUserRoleSuspense } from "@/hooks/use-suspense-queries";
import { usePermissions } from "@/hooks/use-permissions";
import { Suspense, useMemo, useState, useRef, useEffect, useCallback, type ComponentType, type ReactNode } from "react";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { ChevronDown, Check } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { AI_ICON_COLOR, AI_TEXT_GRADIENT } from "@/lib/ai-style";

export const Route = createFileRoute("/_sidebar/settings")({
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
      <CardContent className="space-y-4">
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
const RESERVED_SEVERITY_KEY = "severity";

/** Red asterisk marking a required field — pair with a label, no annotation needed for optional fields. */
function RequiredAsterisk() {
  return (
    <span className="text-destructive" aria-hidden="true">
      *
    </span>
  );
}

/** Small padlock affordance explaining why a reserved field is locked, on hover/focus. */
function LockedFieldHint({ text }: { text: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground cursor-help"
            aria-label={text}
            tabIndex={0}
          >
            <Lock className="h-3 w-3" />
          </span>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs text-xs">
          <p>{text}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

// Draft <-> API mapping (`DraftDefinition` / `defToDraft` / `draftToDef`)
// lives in `lib/label-definition-drafts.ts` so it's unit-testable without
// rendering this route.

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

  const validation = useMemo(() => {
    const errors: string[] = [];
    const seen = new Set<string>();
    for (const d of drafts) {
      const k = d.key.trim();
      if (!k) {
        // A keyless draft is silently not-yet-saveable (the auto-save guard
        // skips it); we don't surface a warning for it.
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

  // Auto-save: debounced 600ms after any change, guarded by validation.
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const triggerAutoSave = useCallback(
    (nextDrafts: DraftDefinition[]) => {
      if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
      const errs: string[] = [];
      const seen = new Set<string>();
      for (const d of nextDrafts) {
        const k = d.key.trim();
        if (!k) { errs.push("missing"); continue; }
        if (!LABEL_KEY_RE.test(k)) errs.push("invalid");
        if (seen.has(k)) errs.push("dup");
        seen.add(k);
      }
      if (errs.length > 0) return;
      saveTimerRef.current = setTimeout(() => {
        const definitions = nextDrafts.map(draftToDef);
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
      }, 600);
    },
    [saveMutation, queryClient, t],
  );

  // Clean up timer on unmount.
  useEffect(() => () => { if (saveTimerRef.current) clearTimeout(saveTimerRef.current); }, []);

  const updateDraft = (draftId: string, patch: Partial<DraftDefinition>) => {
    setDrafts((prev) => {
      const next = prev.map((d) => (d.draftId === draftId ? { ...d, ...patch } : d));
      triggerAutoSave(next);
      return next;
    });
  };

  const removeDraft = (draftId: string) => {
    setDrafts((prev) => {
      const next = prev.filter((d) => d.draftId !== draftId);
      triggerAutoSave(next);
      return next;
    });
  };

  const addDraft = () => {
    setDrafts((prev) => {
      const next = [
        ...prev,
        {
          draftId: crypto.randomUUID(),
          key: "",
          description: "",
          values: [],
          allow_custom_values: false,
          value_colors: null,
          value_descriptions: null,
          value_criticality: null,
          is_builtin: false,
        },
      ];
      // New draft has empty key — auto-save will be triggered once user fills it in.
      return next;
    });
  };

  if (isLoading) {
    return <Skeleton className="h-40 w-full" />;
  }

  const isBuiltinDef = (d: DraftDefinition) =>
    !!d.is_builtin || d.key === RESERVED_WEIGHT_KEY || d.key === RESERVED_SEVERITY_KEY;

  const builtinDrafts = drafts.filter(isBuiltinDef);
  const customDrafts = drafts.filter((d) => !isBuiltinDef(d));

  return (
    <div className="space-y-6">
      {/* Built-in tags */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Tags className="h-5 w-5" />
            {t("config.builtinTagsTitle")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {builtinDrafts.length === 0 && (
            <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
              {t("config.noLabelDefinitions")}
            </div>
          )}
          {builtinDrafts.map((d) => (
            <DefinitionEditorCard
              key={d.draftId}
              draft={d}
              onChange={(patch) => updateDraft(d.draftId, patch)}
              onRemove={() => removeDraft(d.draftId)}
            />
          ))}
        </CardContent>
      </Card>

      {/* Custom tags */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Tags className="h-5 w-5" />
            {t("config.customTagsTitle")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {customDrafts.length === 0 && (
            <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
              {t("config.noLabelDefinitions")}
            </div>
          )}
          {customDrafts.map((d) => (
            <DefinitionEditorCard
              key={d.draftId}
              draft={d}
              onChange={(patch) => updateDraft(d.draftId, patch)}
              onRemove={() => removeDraft(d.draftId)}
            />
          ))}
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="outline" size="sm" onClick={() => addDraft()} className="gap-1.5">
              <Plus className="h-3.5 w-3.5" />
              {t("config.addLabelDefinition")}
            </Button>
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
        </CardContent>
      </Card>
    </div>
  );
}

interface DefinitionEditorCardProps {
  draft: DraftDefinition;
  onChange: (patch: Partial<DraftDefinition>) => void;
  onRemove: () => void;
}

/**
 * Color dot that opens a small popover (native color input + hex field) on
 * click — replaces the old always-visible color box so the collapsed value
 * row stays compact and the picker only appears when needed.
 */
function ValueColorEditor({
  value,
  color,
  onSetColor,
}: {
  value: string;
  color: string | undefined;
  onSetColor: (color: string | null) => void;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [hexText, setHexText] = useState(color ?? "");

  useEffect(() => {
    setHexText(color ?? "");
  }, [color]);

  const commitHex = (raw: string) => {
    const trimmed = raw.trim();
    if (!trimmed) {
      onSetColor(null);
      return;
    }
    const normalised = trimmed.startsWith("#") ? trimmed : `#${trimmed}`;
    if (HEX_COLOR_RE.test(normalised)) {
      onSetColor(normalised);
    } else {
      // Invalid input — revert the text box rather than persisting garbage.
      setHexText(color ?? "");
    }
  };

  const swatchColor = color && HEX_COLOR_RE.test(color) ? color : undefined;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          onClick={(e) => e.stopPropagation()}
          aria-label={t("config.valueColorAria", { value })}
          title={t("config.valueColorTitle", { value })}
          className={cn(
            "h-3.5 w-3.5 shrink-0 rounded-full border cursor-pointer transition-transform hover:scale-125",
            !swatchColor && "border-dashed",
          )}
          style={{ backgroundColor: swatchColor ?? "transparent" }}
        />
      </PopoverTrigger>
      <PopoverContent
        className="w-48 p-2"
        align="start"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2">
          <input
            type="color"
            aria-label={t("config.valueColorAria", { value })}
            value={swatchColor ?? "#94a3b8"}
            onChange={(e) => onSetColor(e.target.value)}
            className="h-7 w-8 cursor-pointer rounded border p-0"
          />
          <Input
            value={hexText}
            onChange={(e) => setHexText(e.target.value)}
            onBlur={(e) => commitHex(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                commitHex(e.currentTarget.value);
              }
            }}
            placeholder={t("config.valueColorPlaceholder")}
            className="h-7 flex-1 text-xs font-mono"
          />
        </div>
      </PopoverContent>
    </Popover>
  );
}

function nextAutoValueName(existing: string[]): string {
  const taken = new Set(existing);
  let i = 1;
  while (taken.has(`value_${i}`)) i++;
  return `value_${i}`;
}

/**
 * Row-based allowed-values editor, modeled on dqlake's ColumnsUsedPanel:
 * each value is a collapsed row (color dot + name + optional description
 * preview) that expands on click into name/color/description inputs.
 * Replaces the old "type a value + press Enter" text box.
 */
function AllowedValuesEditor({
  draft,
  onChange,
}: {
  draft: DraftDefinition;
  onChange: (patch: Partial<DraftDefinition>) => void;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState<number | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (expanded === null) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (rootRef.current?.contains(target)) return;
      if (target.closest("[data-radix-popper-content-wrapper]")) return;
      setExpanded(null);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [expanded]);

  const values = draft.values;
  const colors = draft.value_colors ?? {};
  const descriptions = draft.value_descriptions ?? {};
  const criticalities = draft.value_criticality ?? {};

  // Severity values are stored/seeded lowest-first (Low..Critical) so they
  // read as an ascending scale when authored, but every display surface
  // wants highest-first — see `orderSeverityValuesForDisplay` in
  // RegistryRuleBadges.tsx, which this mirrors for the admin editor. Only
  // the *display* order flips; `patchValue`/`removeAt` below still index
  // into the underlying `values` array by its stored position.
  const isSeverity = draft.key.trim() === RESERVED_SEVERITY_KEY;
  const displayIndices = isSeverity
    ? values.map((_, i) => i).reverse()
    : values.map((_, i) => i);

  const patchValue = (
    i: number,
    patch: { name?: string; color?: string | null; description?: string; criticality?: CriticalityValue },
  ) => {
    const current = values[i];
    if (current === undefined) return;
    let nextValues = values;
    let nextColors = colors;
    let nextDescriptions = descriptions;
    let nextCriticalities = criticalities;

    if (patch.name !== undefined && patch.name !== current) {
      nextValues = values.map((v, idx) => (idx === i ? patch.name! : v));
      if (colors[current] !== undefined) {
        nextColors = { ...colors };
        nextColors[patch.name] = nextColors[current];
        delete nextColors[current];
      }
      if (descriptions[current] !== undefined) {
        nextDescriptions = { ...descriptions };
        nextDescriptions[patch.name] = nextDescriptions[current];
        delete nextDescriptions[current];
      }
      if (criticalities[current] !== undefined) {
        nextCriticalities = { ...criticalities };
        nextCriticalities[patch.name] = nextCriticalities[current];
        delete nextCriticalities[current];
      }
    }
    const targetKey = patch.name ?? current;

    if (patch.color !== undefined) {
      nextColors = { ...nextColors };
      if (patch.color) nextColors[targetKey] = patch.color;
      else delete nextColors[targetKey];
    }
    if (patch.description !== undefined) {
      nextDescriptions = { ...nextDescriptions };
      if (patch.description.trim()) nextDescriptions[targetKey] = patch.description;
      else delete nextDescriptions[targetKey];
    }
    if (patch.criticality !== undefined) {
      nextCriticalities = { ...nextCriticalities, [targetKey]: patch.criticality };
    }

    onChange({
      values: nextValues,
      value_colors: Object.keys(nextColors).length > 0 ? nextColors : null,
      value_descriptions: Object.keys(nextDescriptions).length > 0 ? nextDescriptions : null,
      value_criticality: Object.keys(nextCriticalities).length > 0 ? nextCriticalities : null,
    });
  };

  const removeAt = (i: number) => {
    const current = values[i];
    const nextColors = { ...colors };
    delete nextColors[current];
    const nextDescriptions = { ...descriptions };
    delete nextDescriptions[current];
    const nextCriticalities = { ...criticalities };
    delete nextCriticalities[current];
    onChange({
      values: values.filter((_, idx) => idx !== i),
      value_colors: Object.keys(nextColors).length > 0 ? nextColors : null,
      value_descriptions: Object.keys(nextDescriptions).length > 0 ? nextDescriptions : null,
      value_criticality: Object.keys(nextCriticalities).length > 0 ? nextCriticalities : null,
    });
    if (expanded === i) setExpanded(null);
  };

  const addValue = () => {
    const name = nextAutoValueName(values);
    onChange({ values: [...values, name] });
    setExpanded(values.length);
  };

  return (
    <div ref={rootRef} className="space-y-1.5">
      {displayIndices.map((i) => {
        const v = values[i];
        const isOpen = expanded === i;
        const color = colors[v];
        const description = descriptions[v] ?? "";
        return (
          <div
            key={i}
            className={cn(
              "rounded-md border bg-muted/30 cursor-pointer transition-colors",
              isOpen && "bg-background border-primary/40",
            )}
            onClick={() => setExpanded(isOpen ? null : i)}
          >
            <div
              className={cn(
                "grid items-center gap-2 px-2.5 py-1.5",
                isSeverity ? "grid-cols-[auto_1fr_auto_auto]" : "grid-cols-[auto_1fr_auto]",
              )}
            >
              <ValueColorEditor value={v} color={color} onSetColor={(c) => patchValue(i, { color: c })} />
              <span className="flex items-baseline gap-2 min-w-0">
                <span className="font-mono text-xs shrink-0">{v}</span>
                {description && !isOpen && (
                  <span className="truncate text-xs text-muted-foreground">{description}</span>
                )}
              </span>
              {/* Severity values additionally carry the admin-editable DQX
                  criticality they materialize as (warn/error) — read by the
                  backend's `resolve_criticality`; unset values fall back to
                  the built-in defaults, which is what `resolveCriticality`
                  renders here so the selector always shows the effective
                  criticality. */}
              {isSeverity && (
                <span onClick={(e) => e.stopPropagation()}>
                  <Select
                    value={resolveCriticality(v, draft.value_criticality)}
                    onValueChange={(next) => patchValue(i, { criticality: next as CriticalityValue })}
                  >
                    <SelectTrigger
                      aria-label={t("config.valueCriticalityAria", { value: v })}
                      title={t("config.valueCriticalityTitle", { value: v })}
                      className="h-6 w-24 text-xs"
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="warn" className="text-xs">
                        {t("config.criticalityWarn")}
                      </SelectItem>
                      <SelectItem value="error" className="text-xs">
                        {t("config.criticalityError")}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </span>
              )}
              <button
                type="button"
                aria-label={t("config.removeValueAria", { value: v })}
                className="text-muted-foreground hover:text-destructive transition-colors"
                onClick={(e) => {
                  e.stopPropagation();
                  removeAt(i);
                }}
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
            <AnimatePresence initial={false}>
              {isOpen && (
                <motion.div
                  key="expanded"
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.18, ease: "easeInOut" }}
                  className="overflow-hidden"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-t px-2.5 pb-2.5 pt-2">
                    <div className="flex items-end gap-2">
                      <div className="space-y-1 shrink-0">
                        <Label className="text-xs text-muted-foreground">
                          {t("config.valueLabel")} <RequiredAsterisk />
                        </Label>
                        <Input
                          value={v}
                          onChange={(e) => patchValue(i, { name: e.target.value })}
                          className="h-7 w-40 text-xs font-mono"
                        />
                      </div>
                      <div className="flex-1 space-y-1">
                        <Label className="text-xs text-muted-foreground">{t("config.descriptionLabel")}</Label>
                        <Textarea
                          value={description}
                          onChange={(e) => patchValue(i, { description: e.target.value })}
                          placeholder={t("config.valueDescriptionPlaceholder")}
                          className="w-full text-xs min-h-[28px] py-1 resize-none"
                          rows={1}
                        />
                      </div>
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        );
      })}
      <div className="flex items-center gap-3">
        <Button type="button" variant="outline" size="sm" onClick={addValue} className="h-7 gap-1.5 text-xs">
          <Plus className="h-3.5 w-3.5" />
          {t("config.addValue")}
        </Button>
        {/* Allow-custom-values sits immediately to the right of Add value.
            Reserved keys (dimension/severity) always disallow custom values
            server-side, so it's shown disabled/unchecked for built-ins. */}
        <label
          className={cn(
            "flex items-center gap-1.5 text-sm",
            draft.is_builtin ? "text-muted-foreground" : "cursor-pointer",
          )}
        >
          <Checkbox
            checked={draft.is_builtin ? false : draft.allow_custom_values}
            onCheckedChange={(c) => onChange({ allow_custom_values: c === true })}
            disabled={draft.is_builtin}
          />
          <span>{t("config.allowCustomValues")}</span>
        </label>
      </div>
    </div>
  );
}

function DefinitionEditorCard({ draft, onChange, onRemove }: DefinitionEditorCardProps) {
  const { t } = useTranslation();
  const keyValid = !draft.key || LABEL_KEY_RE.test(draft.key.trim());
  const isWeight = draft.key.trim() === RESERVED_WEIGHT_KEY;
  const isSeverityDef = draft.key.trim() === RESERVED_SEVERITY_KEY;
  const isBuiltin = !!draft.is_builtin;
  const isLocked = isWeight || isBuiltin;

  const descriptionPlaceholder = isWeight
    ? t("config.weightDescriptionPlaceholder")
    : isSeverityDef
      ? t("config.severityDescriptionPlaceholder")
      : t("config.descriptionPlaceholder");

  return (
    <div className="rounded-md border bg-muted/30 p-3 space-y-3">
      {/* Key + Description + delete on one row */}
      <div className="flex items-end gap-2">
        <div className="space-y-1 shrink-0">
          <Label className="text-sm flex items-center gap-1.5">
            {t("config.key")}
            {!isLocked && <RequiredAsterisk />}
            {isLocked && (
              <LockedFieldHint text={isWeight ? t("config.weightHint") : t("config.builtinKeyLockedTooltip")} />
            )}
          </Label>
          <Input
            value={draft.key}
            onChange={(e) => onChange({ key: e.target.value })}
            placeholder={t("config.keyPlaceholder")}
            disabled={isBuiltin}
            className={cn("h-8 w-40 text-xs font-mono", !keyValid && "border-destructive")}
          />
        </div>
        <div className="flex-1 space-y-1">
          <Label className="text-sm">{t("config.descriptionLabel")}</Label>
          <Textarea
            value={draft.description ?? ""}
            onChange={(e) => onChange({ description: e.target.value })}
            placeholder={descriptionPlaceholder}
            disabled={isLocked}
            className="w-full text-xs min-h-[32px] py-1.5 resize-none"
            rows={1}
          />
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-8 w-8 shrink-0 text-destructive hover:text-destructive disabled:opacity-30 mb-0"
          onClick={onRemove}
          disabled={isBuiltin}
          title={isBuiltin ? t("config.builtinCannotDelete") : undefined}
          aria-label={t("config.removeDefinition")}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
      {!keyValid && <p className="text-xs text-destructive">{t("config.keyHint")}</p>}
      {/* Allowed values (the allow-custom-values checkbox lives inline with the
          "Add value" button inside AllowedValuesEditor). */}
      <div className="space-y-1.5">
        <Label className="text-sm">{t("config.allowedValues")}</Label>
        <AllowedValuesEditor draft={draft} onChange={onChange} />
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

type RetentionUnit = "days" | "months" | "years";

function daysToUnit(days: number): { value: number; unit: RetentionUnit } {
  if (days % 365 === 0) return { value: days / 365, unit: "years" };
  if (days % 30 === 0) return { value: days / 30, unit: "months" };
  return { value: days, unit: "days" };
}

function toDays(value: number, unit: RetentionUnit): number {
  if (unit === "years") return value * 365;
  if (unit === "months") return value * 30;
  return value;
}

function RetentionSettings() {
  const { t } = useTranslation();
  const { data, isLoading } = useRetentionSettings();
  const queryClient = useQueryClient();
  const saveMutation = useSaveRetentionSettings();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  const settings = data as RetentionSettingsOut | undefined;

  const [globalValue, setGlobalValue] = useState<string>("");
  const [globalUnit, setGlobalUnit] = useState<RetentionUnit>("months");
  const [quarantineValue, setQuarantineValue] = useState<string>("");
  const [quarantineUnit, setQuarantineUnit] = useState<RetentionUnit>("months");
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (settings && !hydrated) {
      const globalDays = settings.retention_days_set ? settings.retention_days : 90;
      const quarantineDays = settings.quarantine_retention_days_set ? settings.quarantine_retention_days : 30;
      const g = daysToUnit(globalDays);
      const q = daysToUnit(quarantineDays);
      setGlobalValue(String(g.value));
      setGlobalUnit(g.unit);
      setQuarantineValue(String(q.value));
      setQuarantineUnit(q.unit);
      setHydrated(true);
    }
  }, [settings, hydrated]);

  const min = settings?.retention_days_min ?? 7;
  const max = settings?.retention_days_max ?? 3650;

  const saveField = (
    field: "retention_days" | "quarantine_retention_days",
    rawValue: string,
    unit: RetentionUnit,
  ) => {
    if (!settings) return;
    const parsed = Number.parseInt(rawValue, 10);
    if (Number.isNaN(parsed) || parsed <= 0) return;
    const days = Math.min(max, Math.max(min, toDays(parsed, unit)));
    saveMutation.mutate(
      { data: { [field]: days } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getRetentionSettingsQueryKey() });
          toast.success(t("config.retentionSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.failedSaveRetention"));
        },
      },
    );
  };

  if (isLoading || !settings) return <Skeleton className="h-40 w-full" />;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Clock className="h-5 w-5" />
          {t("config.retentionTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          How long runs and quarantined rows are kept
        </p>

        {/* Invalid results — mini card */}
        <div className="flex items-center justify-between gap-4 rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="retention-quarantine" className="text-sm">
              Invalid results
            </Label>
            <p className="text-[11px] text-muted-foreground">
              Applies to quarantined rows, which hold full source-row payloads.
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Input
              id="retention-quarantine"
              type="number"
              min={1}
              step={1}
              value={quarantineValue}
              disabled={!isAdmin || saveMutation.isPending}
              onChange={(e) => setQuarantineValue(e.target.value)}
              onBlur={() => saveField("quarantine_retention_days", quarantineValue, quarantineUnit)}
              className="h-8 w-20"
            />
            <Select
              value={quarantineUnit}
              onValueChange={(v) => {
                const unit = v as RetentionUnit;
                setQuarantineUnit(unit);
                saveField("quarantine_retention_days", quarantineValue, unit);
              }}
              disabled={!isAdmin || saveMutation.isPending}
            >
              <SelectTrigger className="h-8 w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="days">days</SelectItem>
                <SelectItem value="months">months</SelectItem>
                <SelectItem value="years">years</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* All other data — mini card */}
        <div className="flex items-center justify-between gap-4 rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="retention-global" className="text-sm">
              All other data
            </Label>
            <p className="text-[11px] text-muted-foreground">
              Includes run profiling results, history, and metrics.
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Input
              id="retention-global"
              type="number"
              min={1}
              step={1}
              value={globalValue}
              disabled={!isAdmin || saveMutation.isPending}
              onChange={(e) => setGlobalValue(e.target.value)}
              onBlur={() => saveField("retention_days", globalValue, globalUnit)}
              className="h-8 w-20"
            />
            <Select
              value={globalUnit}
              onValueChange={(v) => {
                const unit = v as RetentionUnit;
                setGlobalUnit(unit);
                saveField("retention_days", globalValue, unit);
              }}
              disabled={!isAdmin || saveMutation.isPending}
            >
              <SelectTrigger className="h-8 w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="days">days</SelectItem>
                <SelectItem value="months">months</SelectItem>
                <SelectItem value="years">years</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Draft-run sample limit — admin knob capping the rows a DRAFT monitored-table
// run reads (0 = whole table). Approved/published runs never sample; they
// always scan the full table, so there is deliberately no knob for them.
// ─────────────────────────────────────────────────────────────────────────────

/** Convert draft_sample_limit rows → SampleSelector {kind, value}. */
function limitToSample(limit: number): { kind: SampleKind; value: number } {
  if (limit === 0) return { kind: "full", value: 1000 };
  return { kind: "records", value: limit };
}

/** Convert SampleSelector {kind, value} → draft_sample_limit rows.
 *  Any non-full kind maps to a row count (percent is disabled in this context
 *  via disablePercent, but guard here as belt-and-suspenders). */
function sampleToLimit(kind: SampleKind, value: number): number {
  if (kind === "full") return 0;
  return value; // both "records" and (guarded) "percent" store the numeric value as rows
}

function DraftRunSampleLimitSettings() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { data: resp, isLoading } = useGetDraftRunSampleLimit();
  const settings = resp?.data;
  const saveMutation = useSaveDraftRunSampleLimit();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  const [sampleKind, setSampleKind] = useState<SampleKind>("records");
  const [sampleValue, setSampleValue] = useState(1000);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (settings && !hydrated) {
      const { kind, value } = limitToSample(settings.draft_run_sample_limit);
      setSampleKind(kind);
      setSampleValue(value);
      setHydrated(true);
    }
  }, [settings, hydrated]);

  const handleSave = useCallback((kind: SampleKind, value: number) => {
    const limit = sampleToLimit(kind, value);
    saveMutation.mutate(
      { data: { draft_run_sample_limit: limit } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetDraftRunSampleLimitQueryKey() });
          toast.success(t("config.draftSampleSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.failedSaveDraftSample"));
        },
      },
    );
  }, [saveMutation, queryClient, t]);

  const handleKindChange = (k: SampleKind) => {
    setSampleKind(k);
    handleSave(k, sampleValue);
  };

  const handleValueChange = (n: number) => {
    setSampleValue(n);
    handleSave(sampleKind, n);
  };

  if (isLoading || !settings) return <Skeleton className="h-40 w-full" />;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          {t("config.draftSampleTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label className="text-sm">{t("config.draftSampleLabel")}</Label>
          </div>
          <SampleSelector
            kind={sampleKind}
            value={sampleValue}
            onKind={handleKindChange}
            onValue={handleValueChange}
            disablePercent
          />
        </div>
        {!isAdmin && (
          <span className="text-xs text-muted-foreground">{t("config.draftSampleAdminOnlyHint")}</span>
        )}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Run review statuses — admin-managed catalogue surfaced as the per-run
// review dropdown (Runs detail page) and as a filter on the Runs History
// page. The backend enforces the invariant "exactly one entry has
// is_default == true"; the UI mirrors that with a single radio group
// rather than per-row toggles so the constraint is obvious and the save
// button can stay enabled.
//
// Colors are mapped through a small token table here so the catalogue
// data only ever stores token names ("amber", "green", ...) — the
// design system can rebrand without a data migration.
// ─────────────────────────────────────────────────────────────────────────────

// Token → tailwind classes. Adding a new color elsewhere in the UI
// means adding one entry here; everything else stays data-only.
// Legacy named tokens — retained only to resolve pre-hex stored values to a
// concrete hex (see REVIEW_STATUS_TOKEN_HEX). New colors are stored as #RRGGBB.
const REVIEW_STATUS_COLOR_TOKENS = ["gray", "amber", "green", "blue", "red", "purple"] as const;
type ReviewStatusColorToken = (typeof REVIEW_STATUS_COLOR_TOKENS)[number];

/** Normalise an arbitrary color string to a known token; unknown values fall back to gray. */
function normaliseReviewStatusColor(value: string | undefined | null): ReviewStatusColorToken {
  const lower = (value || "").trim().toLowerCase();
  return (REVIEW_STATUS_COLOR_TOKENS as readonly string[]).includes(lower)
    ? (lower as ReviewStatusColorToken)
    : "gray";
}

// Concrete hex for each legacy token, so a stored token ("gray", "amber", …)
// and a hex value ("#ef4444") both resolve to a hex we can render inline.
const REVIEW_STATUS_TOKEN_HEX: Record<ReviewStatusColorToken, string> = {
  gray: "#94a3b8",
  amber: "#f59e0b",
  green: "#22c55e",
  blue: "#3b82f6",
  red: "#ef4444",
  purple: "#a855f7",
};

/** Resolve a stored review-status color (hex OR legacy token) to a #RRGGBB hex. */
export function reviewStatusColorHex(color: string | undefined | null): string {
  const trimmed = (color || "").trim();
  if (HEX_COLOR_RE.test(trimmed)) return trimmed;
  return REVIEW_STATUS_TOKEN_HEX[normaliseReviewStatusColor(trimmed)];
}

/** Inline badge style derived from the resolved hex — a subtle tint that works
 *  for arbitrary hex colors (Tailwind classes can't encode a runtime hex). */
export function reviewStatusBadgeStyle(color: string | undefined | null): React.CSSProperties {
  const hex = reviewStatusColorHex(color);
  return { backgroundColor: `${hex}1f`, color: hex, borderColor: `${hex}66` };
}

export { REVIEW_STATUS_COLOR_TOKENS };

/** Normalise a draft array so exactly one entry has is_default=true.
 *  Priority: (1) "Pending review" row (case-insensitive trim),
 *            (2) first existing default,
 *            (3) first row.
 *  Returns a new array; safe to call with empty arrays (returns []).
 */
function normaliseReviewStatusDefaults(statuses: RunReviewStatusOption[]): RunReviewStatusOption[] {
  if (statuses.length === 0) return [];
  const pendingIdx = statuses.findIndex((s) => s.value.trim().toLowerCase() === "pending review");
  const existingDefaultIdx = statuses.findIndex((s) => s.is_default);
  const defaultIdx = pendingIdx >= 0 ? pendingIdx : existingDefaultIdx >= 0 ? existingDefaultIdx : 0;
  return statuses.map((s, i) => ({ ...s, is_default: i === defaultIdx }));
}

function RunReviewStatusesSettings() {
  const { t } = useTranslation();
  const { data, isLoading } = useRunReviewStatuses();
  const queryClient = useQueryClient();
  const saveMutation = useSaveRunReviewStatuses();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  // Local working copy; never mutated in-place.
  const [draft, setDraft] = useState<RunReviewStatusOption[]>([]);
  const [hydrated, setHydrated] = useState(false);
  const [expanded, setExpanded] = useState<number | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (data && !hydrated) {
      // Normalise on hydration: ensure exactly one default, preferring
      // "Pending review" → first existing default → first row.
      setDraft(normaliseReviewStatusDefaults(data.statuses.map((s) => ({ ...s }))));
      setHydrated(true);
    }
  }, [data, hydrated]);

  // Collapse expanded row when clicking outside.
  useEffect(() => {
    if (expanded === null) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (rootRef.current?.contains(target)) return;
      if (target.closest("[data-radix-popper-content-wrapper]")) return;
      setExpanded(null);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [expanded]);

  // The save endpoint enforces exactly-one-default; mirror that in the
  // UI so auto-save is only triggered when the constraint is met.
  const validationError = useMemo<string | null>(() => {
    if (draft.length === 0) {
      return "At least one status is required.";
    }
    const seen = new Set<string>();
    for (const entry of draft) {
      const trimmed = entry.value.trim();
      if (!trimmed) return "Every status needs a value.";
      if (!/^[A-Za-z0-9][A-Za-z0-9 _\-/.]{0,79}$/.test(trimmed)) {
        return `Invalid value "${trimmed}". Use letters, digits, spaces, hyphens (max 80 chars).`;
      }
      if (seen.has(trimmed)) return `Duplicate value: "${trimmed}".`;
      seen.add(trimmed);
    }
    const defaults = draft.filter((d) => d.is_default).length;
    if (defaults === 0) return "Pick one status as the default.";
    if (defaults > 1) return "Only one status can be marked default.";
    return null;
  }, [draft]);

  // Auto-save: debounced 600ms after any change, guarded by validation.
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const triggerAutoSave = useCallback(
    (nextDraft: RunReviewStatusOption[]) => {
      if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
      // Run validation inline — same rules as validationError memo.
      if (nextDraft.length === 0) return;
      const seen = new Set<string>();
      for (const entry of nextDraft) {
        const trimmed = entry.value.trim();
        if (!trimmed) return;
        if (!/^[A-Za-z0-9][A-Za-z0-9 _\-/.]{0,79}$/.test(trimmed)) return;
        if (seen.has(trimmed)) return;
        seen.add(trimmed);
      }
      const defaults = nextDraft.filter((d) => d.is_default).length;
      if (defaults !== 1) return;
      saveTimerRef.current = setTimeout(() => {
        const cleaned = nextDraft.map((entry) => ({
          value: entry.value.trim(),
          description: (entry.description || "").trim(),
          // Persist a concrete #RRGGBB hex (resolving any legacy token too) so
          // the picker round-trips and the badges render the chosen color.
          color: reviewStatusColorHex(entry.color),
          is_default: Boolean(entry.is_default),
        }));
        saveMutation.mutate(
          { data: { statuses: cleaned } },
          {
            onSuccess: (resp) => {
              queryClient.invalidateQueries({ queryKey: getRunReviewStatusesQueryKey() });
              setDraft(resp.data.statuses.map((s) => ({ ...s })));
              toast.success(t("config.reviewStatusesSaved"));
            },
            onError: (err: unknown) => {
              const axErr = err as AxiosError<{ detail?: string }>;
              toast.error(axErr?.response?.data?.detail ?? t("config.failedSaveReviewStatuses"));
            },
          },
        );
      }, 600);
    },
    [saveMutation, queryClient, t],
  );

  // Clean up timer on unmount.
  useEffect(() => () => { if (saveTimerRef.current) clearTimeout(saveTimerRef.current); }, []);

  const handleAdd = () => {
    const next = [...draft, { value: "", description: "", color: "gray", is_default: false }];
    setDraft(next);
    setExpanded(next.length - 1);
    triggerAutoSave(next);
  };

  const handleRemove = (idx: number) => {
    const filtered = draft.filter((_, i) => i !== idx);
    // Re-assign default if we removed the default row.
    const next = normaliseReviewStatusDefaults(filtered);
    setDraft(next);
    if (expanded === idx) setExpanded(null);
    triggerAutoSave(next);
  };

  const handlePatch = (idx: number, patch: Partial<RunReviewStatusOption>) => {
    const next = draft.map((entry, i) => (i === idx ? { ...entry, ...patch } : entry));
    setDraft(next);
    triggerAutoSave(next);
  };

  if (isLoading || !data) {
    return <Skeleton className="h-40 w-full" />;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldCheck className="h-5 w-5" />
          {t("config.reviewStatusesTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          Statuses used when reviewing results of each run via the Results tab
        </p>

        <div ref={rootRef} className="space-y-1.5">
          {draft.map((entry, idx) => {
            const isOpen = expanded === idx;
            return (
              <div
                key={idx}
                className={cn(
                  "rounded-md border bg-muted/30 cursor-pointer transition-colors",
                  isOpen && "bg-background border-primary/40",
                )}
                onClick={() => setExpanded(isOpen ? null : idx)}
              >
                {/* Collapsed row — swatch is a click-to-pick hex color, like the
                    Tags allowed-values editor. */}
                <div className="grid grid-cols-[auto_1fr_auto] items-center gap-2 px-2.5 py-1.5">
                  <ValueColorEditor
                    value={entry.value || t("config.reviewStatusFallback")}
                    color={reviewStatusColorHex(entry.color)}
                    onSetColor={(c) => handlePatch(idx, { color: c ?? reviewStatusColorHex(null) })}
                  />
                  <span className="flex items-baseline gap-2 min-w-0">
                    <span className="font-mono text-xs shrink-0">{entry.value || <span className="italic text-muted-foreground">{t("config.reviewStatusUnnamed")}</span>}</span>
                    {entry.description && !isOpen && (
                      <span className="truncate text-[11px] text-muted-foreground">{entry.description}</span>
                    )}
                  </span>
                  <button
                    type="button"
                    aria-label={t("config.reviewStatusRemoveAria", { value: entry.value || t("config.reviewStatusFallback") })}
                    className="text-muted-foreground hover:text-destructive transition-colors"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleRemove(idx);
                    }}
                    disabled={!isAdmin || saveMutation.isPending || draft.length <= 1}
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
                {/* Expanded inline editor */}
                <AnimatePresence initial={false}>
                  {isOpen && (
                    <motion.div
                      key="expanded"
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: "auto", opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      transition={{ duration: 0.18, ease: "easeInOut" }}
                      className="overflow-hidden"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="border-t px-2.5 pb-2.5 pt-2 space-y-2">
                        <div className="flex items-end gap-2">
                          {/* Value input (color is picked via the swatch on the
                              collapsed row, matching the Tags editor) */}
                          <div className="space-y-1 shrink-0">
                            <Label className="text-xs text-muted-foreground">
                              Value <RequiredAsterisk />
                            </Label>
                            <Input
                              value={entry.value}
                              onChange={(e) => handlePatch(idx, { value: e.target.value })}
                              placeholder="e.g. Confirmed"
                              maxLength={80}
                              disabled={!isAdmin || saveMutation.isPending}
                              className="h-7 w-40 text-xs font-mono"
                              autoComplete="off"
                            />
                          </div>
                          {/* Description input */}
                          <div className="flex-1 space-y-1">
                            <Label className="text-xs text-muted-foreground">Description</Label>
                            <Input
                              value={entry.description}
                              onChange={(e) => handlePatch(idx, { description: e.target.value })}
                              placeholder="Shown as a tooltip on the dropdown"
                              maxLength={200}
                              disabled={!isAdmin || saveMutation.isPending}
                              className="h-7 text-xs"
                            />
                          </div>
                        </div>
                        {/* Default indicator — locked ON for the default row, hidden for all others */}
                        {entry.is_default && (
                          <span className="flex items-center gap-1.5 text-xs text-primary font-medium">
                            <CheckCircle2 className="h-3.5 w-3.5" /> Default for new runs
                          </span>
                        )}
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            );
          })}
        </div>

        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={handleAdd}
          disabled={!isAdmin || saveMutation.isPending}
          className="h-7 gap-1.5 text-xs"
        >
          <Plus className="h-3.5 w-3.5" />
          Add status
        </Button>

        {validationError && (
          <p className="text-[11px] text-destructive flex items-center gap-1">
            <AlertCircle className="h-3 w-3" />
            {validationError}
          </p>
        )}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// AI settings (Rules Registry Phase 4.5, trimmed to essentials in 8B) — the
// kill-switch + serving endpoint for AIGateway (Build-with-AI / per-field
// suggest). ADMIN only — every AI affordance elsewhere in the app degrades
// to hidden/disabled while these are unset (see hooks/use-ai-availability.ts).
//
// The rule-mapping suggester's Vector Search settings (embedding endpoint,
// VS endpoint/index) are no longer surfaced here — they auto-derive from
// this toggle + endpoint alone (see ``AppSettingsService`` on the backend),
// so the suggester is always-on whenever AI is enabled, no extra fields to
// fill in.
// ─────────────────────────────────────────────────────────────────────────────

// Sentinel values for Radix Select — it rejects empty-string item values.
const NO_ENDPOINT_VALUE = "__none__";
const NO_WAREHOUSE_VALUE = "__default__";
const NO_CLUSTER_VALUE = "__none__";

/**
 * SQL-warehouse dropdown. Groups warehouses into Serverless and Classic with
 * green/red status dots (mirrors dqlake EvalWarehouseCard). Always includes
 * the currently-configured id even when the workspace list hasn't loaded yet
 * (or the warehouse was since deleted), so Radix Select never receives a
 * value with no matching item.
 */
function WarehouseSelect({
  value,
  onChange,
  warehouses,
  disabled,
}: {
  value: string;
  onChange: (value: string) => void;
  warehouses: WarehouseOut[];
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const { serverless, classic } = useMemo(() => {
    const byId = new Map(warehouses.map((w) => [w.id, w]));
    // Ensure saved id is always present in the list even if not in workspace
    if (value && !byId.has(value)) {
      byId.set(value, { id: value, name: value, serverless: false, running: false });
    }
    const all = Array.from(byId.values()).sort((a, b) => a.name.localeCompare(b.name));
    return {
      serverless: all.filter((w) => w.serverless),
      classic: all.filter((w) => !w.serverless),
    };
  }, [warehouses, value]);

  return (
    <Select
      value={value || NO_WAREHOUSE_VALUE}
      onValueChange={(v) => onChange(v === NO_WAREHOUSE_VALUE ? "" : v)}
      disabled={disabled}
    >
      <SelectTrigger className="h-8 text-xs w-52">
        <SelectValue placeholder={t("config.computeWarehousePlaceholder")} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={NO_WAREHOUSE_VALUE} className="text-xs">
          {t("config.computeWarehouseDefault")}
        </SelectItem>
        {serverless.length > 0 && (
          <>
            <SelectLabel className="text-[10px] uppercase tracking-wide text-muted-foreground px-2 pt-1">
              {t("config.computeWarehouseServerlessGroup")}
            </SelectLabel>
            {serverless.map((w) => (
              <SelectItem key={w.id} value={w.id} className="text-xs">
                <span className="flex items-center gap-1.5">
                  <span className="h-1.5 w-1.5 rounded-full bg-green-500 shrink-0" />
                  {w.name}
                  {w.id === value && !warehouses.some((x) => x.id === value)
                    ? ` (${t("config.computeWarehouseCustomOption")})`
                    : ""}
                </span>
              </SelectItem>
            ))}
          </>
        )}
        {classic.length > 0 && (
          <>
            <SelectLabel className="text-[10px] uppercase tracking-wide text-muted-foreground px-2 pt-1">
              {t("config.computeWarehouseClassicGroup")}
            </SelectLabel>
            {classic.map((w) => (
              <SelectItem key={w.id} value={w.id} className="text-xs">
                <span className="flex items-center gap-1.5">
                  <span className={cn("h-1.5 w-1.5 rounded-full shrink-0", w.running ? "bg-green-500" : "bg-red-500")} />
                  {w.name}
                  {w.id === value && !warehouses.some((x) => x.id === value)
                    ? ` (${t("config.computeWarehouseCustomOption")})`
                    : ""}
                </span>
              </SelectItem>
            ))}
          </>
        )}
        {serverless.length === 0 && classic.length === 0 && warehouses.length === 0 && (
          <SelectLabel className="font-normal">{t("config.computeNoWarehouses")}</SelectLabel>
        )}
      </SelectContent>
    </Select>
  );
}

/**
 * All-purpose cluster dropdown for jobs compute. Mirrors {@link WarehouseSelect}:
 * always exposes a "none" sentinel item and keeps the saved cluster id selectable
 * even when the list is empty or still loading.
 */
function ClusterSelect({
  value,
  onChange,
  clusters,
  clustersError,
  disabled,
}: {
  value: string;
  onChange: (value: string) => void;
  clusters: ClusterOut[];
  clustersError?: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const options = useMemo(() => {
    const byId = new Map(clusters.map((c) => [c.cluster_id, c]));
    if (value && !byId.has(value)) {
      byId.set(value, { cluster_id: value, cluster_name: value, state: "" });
    }
    return Array.from(byId.values()).sort((a, b) => a.cluster_name.localeCompare(b.cluster_name));
  }, [clusters, value]);

  return (
    <Select
      value={value || NO_CLUSTER_VALUE}
      onValueChange={(v) => onChange(v === NO_CLUSTER_VALUE ? "" : v)}
      disabled={disabled}
    >
      <SelectTrigger className="h-8 text-xs w-52 mt-1.5">
        <SelectValue placeholder={t("config.computeClusterPlaceholder")} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={NO_CLUSTER_VALUE} className="text-xs">
          {t("config.computeClusterNone")}
        </SelectItem>
        {options.length === 0 && (
          <SelectLabel className="font-normal">
            {clustersError ? t("config.computeClustersUnavailable") : t("config.computeNoClusters")}
          </SelectLabel>
        )}
        {options.map((c) => (
          <SelectItem key={c.cluster_id} value={c.cluster_id} className="text-xs">
            <span className="flex items-center gap-1.5">
              <span className={cn("h-1.5 w-1.5 rounded-full shrink-0", c.state === "RUNNING" ? "bg-green-500" : "bg-red-500")} />
              {c.cluster_name}
              {c.cluster_id === value && !clusters.some((x) => x.cluster_id === value)
                ? ` (${t("config.computeClusterCustomOption")})`
                : ""}
            </span>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

/**
 * Serving-endpoint dropdown for the AI endpoint field. Always includes the
 * currently-configured value as an option even if it's missing from the
 * fetched workspace list (e.g. typed before this dropdown existed, or the
 * endpoint was since deleted) so saving doesn't silently blank out an
 * existing setting.
 */
function ServingEndpointSelect({
  value,
  onChange,
  endpoints,
  disabled,
}: {
  value: string;
  onChange: (value: string) => void;
  endpoints: string[];
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const options = useMemo(() => {
    const set = new Set(endpoints);
    if (value) set.add(value);
    return Array.from(set).sort();
  }, [endpoints, value]);

  const selectValue = value || NO_ENDPOINT_VALUE;

  return (
    <Select
      value={selectValue}
      onValueChange={(v) => onChange(v === NO_ENDPOINT_VALUE ? "" : v)}
      disabled={disabled}
    >
      <SelectTrigger className="h-8 text-xs font-mono w-full">
        <SelectValue placeholder={t("config.aiSettingsSelectEndpointPlaceholder")} />
      </SelectTrigger>
      <SelectContent>
        {options.length === 0 && (
          <SelectLabel className="font-normal">{t("config.aiSettingsNoEndpointsFound")}</SelectLabel>
        )}
        {options.map((name) => {
          const isCustom = !endpoints.includes(name);
          return (
            <SelectItem key={name} value={name} className="text-xs font-mono">
              {isCustom ? t("config.aiSettingsCustomEndpointOption", { value: name }) : name}
            </SelectItem>
          );
        })}
      </SelectContent>
    </Select>
  );
}

function AiSettingsCard() {
  const { t } = useTranslation();
  const { data: settingsResp, isLoading } = useGetAiSettings();
  const data = settingsResp?.data;
  const queryClient = useQueryClient();
  const saveMutation = useSaveAiSettings();
  const ensureVectorStoreMutation = useEnsureVectorStore();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";
  const { data: servingEndpointsResp } = useListServingEndpoints();
  const servingEndpoints = useMemo(() => servingEndpointsResp?.data.names ?? [], [servingEndpointsResp]);
  const { data: workspace } = useWorkspaceHost();
  const host = workspace?.workspace_host;

  const [aiEnabled, setAiEnabled] = useState(false);
  const [aiEndpoint, setAiEndpoint] = useState("");
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (data && !hydrated) {
      setAiEnabled(data.ai_enabled);
      setAiEndpoint(data.ai_endpoint_name);
      setHydrated(true);
    }
  }, [data, hydrated]);

  const doSave = (payload: AiSettingsIn, enabledForVS: boolean) => {
    saveMutation.mutate(
      { data: payload },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetAiSettingsQueryKey() });
          toast.success(t("config.aiSettingsSaved"));
          // Fire-and-forget: kick off Vector Search endpoint/index creation
          // now that AI is enabled — the embedding/VS names auto-derive
          // server-side, so the only gate is the enable toggle. Provisioning
          // is async and can take minutes, so this call is never awaited by
          // the UI — the suggester reports readiness separately once the
          // index comes online. Silently ignore failures; this is
          // best-effort.
          if (enabledForVS) {
            ensureVectorStoreMutation.mutate(undefined, { onError: () => {} });
          }
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.aiSettingsSaveFailed"));
        },
      },
    );
  };

  const handleEnabledChange = (checked: boolean) => {
    setAiEnabled(checked);
    doSave({ ai_enabled: checked, ai_endpoint_name: aiEndpoint.trim() }, checked);
  };

  const handleEndpointChange = (value: string) => {
    setAiEndpoint(value);
    doSave({ ai_enabled: aiEnabled, ai_endpoint_name: value.trim() }, aiEnabled);
  };

  if (isLoading || !data) {
    return <Skeleton className="h-40 w-full" />;
  }

  const manageHref = host
    ? aiEndpoint
      ? `${host}/ml/endpoints/${encodeURIComponent(aiEndpoint)}`
      : `${host}/ml/endpoints`
    : undefined;

  return (
    <Card className="border-fuchsia-500/30 dark:border-fuchsia-400/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Sparkles className={cn("h-5 w-5", AI_ICON_COLOR)} />
          <span className={cn("font-bold", AI_TEXT_GRADIENT)}>{t("config.aiSettingsTitle")}</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-md border border-fuchsia-500/30 dark:border-fuchsia-400/30 p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="ai-settings-enabled" className="text-sm">{t("config.aiSettingsEnabledLabel")}</Label>
            <p className="text-[11px] text-muted-foreground">{t("config.aiSettingsEnabledHint")}</p>
          </div>
          <Switch
            id="ai-settings-enabled"
            checked={aiEnabled}
            onCheckedChange={handleEnabledChange}
            disabled={!isAdmin || saveMutation.isPending}
            className="data-[state=checked]:bg-fuchsia-500"
          />
        </div>

        <div className="flex items-center justify-between gap-4 rounded-md border border-fuchsia-500/30 dark:border-fuchsia-400/30 p-3">
          <div className="space-y-0.5">
            <Label className="text-sm">{t("config.aiSettingsEndpointLabel")}</Label>
            <p className="text-[11px] text-muted-foreground">{t("config.aiSettingsEndpointHint")}</p>
            {manageHref && (
              <a
                href={manageHref}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-primary hover:underline inline-flex items-center gap-1"
              >
                {t("config.aiSettingsManageLink")}
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>
          <div className="w-64 shrink-0">
            <ServingEndpointSelect
              value={aiEndpoint}
              onChange={handleEndpointChange}
              endpoints={servingEndpoints}
              disabled={!isAdmin || !aiEnabled || saveMutation.isPending}
            />
          </div>
        </div>

        {!isAdmin && <span className="text-xs text-muted-foreground">{t("config.aiSettingsAdminOnlyHint")}</span>}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Rules Registry governance — two distinct admin knobs bundled behind one
// read/write surface (P21-G): ``defaultAutoUpgrade`` governs the pin chosen
// at ATTACH TIME for a brand-new rule application / data-product member;
// ``autoUpgradeWithoutApproval`` governs RE-APPROVAL of an EXISTING
// following application when its rule is re-published. Kept side by side so
// stewards don't conflate "when does a table start following a rule" with
// "what happens once it's already following it".
// ─────────────────────────────────────────────────────────────────────────────

function RulesRegistrySettingsCard() {
  const { t } = useTranslation();
  const { data, isLoading } = useGetRulesRegistrySettings();
  const queryClient = useQueryClient();
  const saveMutation = useSaveRulesRegistrySettings();
  const { isAdmin } = usePermissions();

  const settings = data?.data;

  if (isLoading || !settings) return <Skeleton className="h-40 w-full" />;

  const save = (payload: RulesRegistrySettingsIn) => {
    saveMutation.mutate(
      { data: payload },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetRulesRegistrySettingsQueryKey() });
          toast.success(t("config.rulesRegistrySettingsSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.rulesRegistrySettingsFailedSave"));
        },
      },
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldCheck className="h-5 w-5" />
          {t("config.rulesRegistrySettingsTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          {t("config.rulesRegistrySettingsDescription")}
        </p>

        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="default-auto-upgrade" className="text-sm">
              {t("config.defaultAutoUpgradeLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">
              {settings.default_auto_upgrade
                ? t("config.defaultAutoUpgradeHint")
                : t("config.defaultAutoUpgradeHintOff")}
            </p>
          </div>
          <Switch
            id="default-auto-upgrade"
            checked={settings.default_auto_upgrade}
            onCheckedChange={(checked) => save({ default_auto_upgrade: checked })}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="tag-auto-apply" className="text-sm">
              {t("config.tagAutoApplyLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">{t("config.tagAutoApplyHint")}</p>
          </div>
          <Switch
            id="tag-auto-apply"
            checked={settings.tag_auto_apply}
            onCheckedChange={(checked) => save({ tag_auto_apply: checked })}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        {!isAdmin && (
          <span className="text-xs text-muted-foreground">{t("config.rulesRegistrySettingsAdminOnlyHint")}</span>
        )}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Approvals mode (#94) — the app-wide submit→approve gate. A 3-state select:
// enabled (author submits, approver approves), auto_bypass (submit auto-approves
// when the caller could approve it themselves), disabled (every submit
// auto-approves). Read by all submit/approve surfaces to pick the right button.
// ─────────────────────────────────────────────────────────────────────────────

const APPROVAL_MODES = ["enabled", "auto_bypass", "disabled"] as const;

function ApprovalsModeCard() {
  const { t } = useTranslation();
  const { data, isLoading } = useGetApprovalsMode();
  const { data: registryData, isLoading: registryLoading } = useGetRulesRegistrySettings();
  const queryClient = useQueryClient();
  const saveMutation = useSaveApprovalsMode();
  const saveRegistryMutation = useSaveRulesRegistrySettings();
  const { isAdmin } = usePermissions();

  const mode = data?.data?.mode;
  const registrySettings = registryData?.data;

  if (isLoading || !mode || registryLoading || !registrySettings) return <Skeleton className="h-40 w-full" />;

  const save = (next: string) => {
    saveMutation.mutate(
      { data: { mode: next } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetApprovalsModeQueryKey() });
          toast.success(t("config.approvalsModeSaved"));
          // When approvals mode is disabled, turn off bypass setting
          if (next === "disabled") {
            saveRegistryMutation.mutate(
              { data: { auto_upgrade_without_approval: false } },
              {
                onSuccess: () => {
                  queryClient.invalidateQueries({ queryKey: getGetRulesRegistrySettingsQueryKey() });
                },
                onError: (err: unknown) => {
                  const axErr = err as AxiosError<{ detail?: string }>;
                  toast.error(axErr?.response?.data?.detail ?? t("config.rulesRegistrySettingsFailedSave"));
                },
              },
            );
          }
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.approvalsModeFailedSave"));
        },
      },
    );
  };

  const saveBypass = (checked: boolean) => {
    saveRegistryMutation.mutate(
      { data: { auto_upgrade_without_approval: checked } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetRulesRegistrySettingsQueryKey() });
          toast.success(t("config.rulesRegistrySettingsSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.rulesRegistrySettingsFailedSave"));
        },
      },
    );
  };

  const approvalsDisabled = mode === "disabled";

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldCheck className="h-5 w-5" />
          {t("config.approvalsModeTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          {t("config.approvalsModeDescription")}
        </p>

        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="approvals-mode" className="text-sm">
              {t("config.approvalsModeLabel")}
            </Label>
            {mode !== "enabled" && (
              <p className="text-[11px] text-muted-foreground">
                {t(`config.approvalsMode_${mode}_hint`)}
              </p>
            )}
          </div>
          <Select value={mode} onValueChange={save} disabled={!isAdmin || saveMutation.isPending}>
            <SelectTrigger id="approvals-mode" className="h-8 w-52 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {APPROVAL_MODES.map((m) => (
                <SelectItem key={m} value={m} className="text-xs">
                  {t(`config.approvalsMode_${m}_option`)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="auto-upgrade-without-approval" className="text-sm">
              {t("config.autoUpgradeWithoutApprovalLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">
              {t("config.autoUpgradeWithoutApprovalHint")}
            </p>
          </div>
          <Switch
            id="auto-upgrade-without-approval"
            checked={approvalsDisabled ? false : registrySettings.auto_upgrade_without_approval}
            onCheckedChange={saveBypass}
            disabled={!isAdmin || saveMutation.isPending || saveRegistryMutation.isPending || approvalsDisabled}
          />
        </div>

        {!isAdmin && (
          <span className="text-xs text-muted-foreground">{t("config.approvalsModeAdminOnlyHint")}</span>
        )}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Require a draft run before submit (issue B2-12). When on, a monitored table /
// table space / per-table rule cannot be submitted for review (nor take the
// approvals-mode auto-approve shortcut) until a draft run has been recorded for
// its target table(s). Registry rules and cross-table SQL checks are
// table-agnostic and are never gated.
// ─────────────────────────────────────────────────────────────────────────────

function RequireDraftRunSettingsCard() {
  const { t } = useTranslation();
  const { data, isLoading } = useGetRequireDraftRunSettings({ query: { select: (d) => d.data } });
  const queryClient = useQueryClient();
  const saveMutation = useSaveRequireDraftRunSettings();
  const { isAdmin } = usePermissions();

  if (isLoading || !data) return <Skeleton className="h-40 w-full" />;

  const save = (enabled: boolean) => {
    saveMutation.mutate(
      { data: { require_draft_run_before_submit: enabled } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetRequireDraftRunSettingsQueryKey() });
          toast.success(t("config.requireDraftRunSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.requireDraftRunFailedSave"));
        },
      },
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <FlaskConical className="h-5 w-5" />
          {t("config.requireDraftRunTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="require-draft-run" className="text-sm">
              {t("config.requireDraftRunLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">{t("config.requireDraftRunHint")}</p>
          </div>
          <Switch
            id="require-draft-run"
            checked={data.require_draft_run_before_submit}
            onCheckedChange={(checked) => save(checked)}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        {!isAdmin && (
          <span className="text-xs text-muted-foreground">{t("config.requireDraftRunAdminOnlyHint")}</span>
        )}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Permissions — admin default for the per-grant inheritance toggle. When on,
// a new grant on a table space defaults to flowing down to its member tables.
// Individual grants can still override this per-grant in the Permissions tab.
// ─────────────────────────────────────────────────────────────────────────────

function PermissionsSettingsCard() {
  const { t } = useTranslation();
  const { data, isLoading } = useGetPermissionsDefaultInherit({ query: { select: (d) => d.data } });
  const queryClient = useQueryClient();
  const saveMutation = useSetPermissionsDefaultInherit();
  const { isAdmin } = usePermissions();

  if (isLoading || !data) return <Skeleton className="h-40 w-full" />;

  const save = (enabled: boolean) => {
    saveMutation.mutate(
      { data: { enabled } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetPermissionsDefaultInheritQueryKey() });
          toast.success(t("config.permissionsDefaultInheritSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.permissionsDefaultInheritFailedSave"));
        },
      },
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <KeyRound className="h-5 w-5" />
          {t("config.permissionsDefaultInheritTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="permissions-default-inherit" className="text-sm">
              {t("config.permissionsDefaultInheritLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">{t("config.permissionsDefaultInheritHint")}</p>
          </div>
          <Switch
            id="permissions-default-inherit"
            checked={data.enabled}
            onCheckedChange={(checked) => save(checked)}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        {!isAdmin && (
          <span className="text-xs text-muted-foreground">{t("config.permissionsDefaultInheritAdminOnlyHint")}</span>
        )}
      </CardContent>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Global Results tab (B2-20) — admin opt-in for the app-wide, all-tables
// Results surface. OFF by default: it duplicates the per-object results tabs
// and confuses fresh deploys. When enabled, the global Results sidebar nav
// item AND the homepage overall-score "?" explainer appear. Per-object MT/TS/RR
// results tabs are unaffected.
// ─────────────────────────────────────────────────────────────────────────────

function GlobalResultsSettingsCard() {
  const { t } = useTranslation();
  const { data, isLoading } = useGetGlobalResultsSettings({ query: { select: (d) => d.data } });
  const queryClient = useQueryClient();
  const saveMutation = useSaveGlobalResultsSettings();
  const { isAdmin } = usePermissions();

  if (isLoading || !data) return <Skeleton className="h-40 w-full" />;

  const save = (enabled: boolean) => {
    saveMutation.mutate(
      { data: { global_results_enabled: enabled } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetGlobalResultsSettingsQueryKey() });
          toast.success(t("config.globalResultsSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.globalResultsFailedSave"));
        },
      },
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <LineChart className="h-5 w-5" />
          {t("config.globalResultsTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="global-results-enabled" className="text-sm">
              {t("config.globalResultsLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">{t("config.globalResultsHint")}</p>
          </div>
          <Switch
            id="global-results-enabled"
            checked={data.global_results_enabled}
            onCheckedChange={(checked) => save(checked)}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        {!isAdmin && (
          <span className="text-xs text-muted-foreground">{t("config.globalResultsAdminOnlyHint")}</span>
        )}
      </CardContent>
    </Card>
  );
}

/** Inner content rendered after all suspense data is available. */
function ComputeSettingsContent() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { data: settingsResp } = useGetComputeSettingsSuspense();
  const settings = settingsResp.data;
  const { data: warehousesResp } = useListComputeWarehousesSuspense();
  const warehouses = useMemo(() => warehousesResp.data ?? [], [warehousesResp]);
  // The clusters list is best-effort: the backend swallows a missing-scope /
  // permission error to `[]` (200). Hard transport errors are caught by the
  // Suspense error boundary. Either way we degrade gracefully.
  const { data: clustersResp } = useListComputeClustersSuspense();
  const clusters = useMemo(() => clustersResp.data ?? [], [clustersResp]);
  const { isAdmin } = usePermissions();

  const saveMutation = useSaveComputeSettings();
  const grantMutation = useGrantWarehouseAccess();

  // Local state mirrors the saved settings — initialised directly from
  // suspense data (always available by the time this component renders).
  const [warehouseId, setWarehouseId] = useState(() => settings.sql_warehouse_id ?? "");
  const [jobsKind, setJobsKind] = useState<JobsComputeModel["kind"]>(() => settings.jobs_compute?.kind ?? "serverless");
  const [clusterId, setClusterId] = useState(() => settings.jobs_compute?.cluster_id ?? "");

  // Keep a ref to current field values so save() never reads stale closure state.
  const currentRef = useRef({ warehouseId: settings.sql_warehouse_id ?? "", jobsKind: settings.jobs_compute?.kind ?? "serverless" as JobsComputeModel["kind"], clusterId: settings.jobs_compute?.cluster_id ?? "" });
  currentRef.current = { warehouseId, jobsKind, clusterId };

  // The warehouse whose SP access we check: the picked one, else the effective
  // (env-fallback) warehouse. Re-runs whenever the pick changes.
  const checkWarehouseId = warehouseId || settings.effective_warehouse_id || "";
  const { data: accessResp } = useGetWarehouseAccess(
    { warehouse_id: checkWarehouseId },
    { query: { enabled: !!checkWarehouseId } },
  );
  const access = accessResp?.data;

  const save = useCallback((patch: { warehouseId?: string; kind?: JobsComputeModel["kind"]; clusterId?: string }) => {
    // Read siblings from the ref so we never close over stale state.
    const resolvedKind = patch.kind ?? currentRef.current.jobsKind;
    const resolvedCluster = patch.clusterId ?? currentRef.current.clusterId;
    const resolvedWarehouse = patch.warehouseId ?? currentRef.current.warehouseId;
    const jobs_compute: JobsComputeModel =
      resolvedKind === "existing_cluster"
        ? { kind: "existing_cluster", cluster_id: resolvedCluster || null }
        : { kind: "serverless", cluster_id: null };
    const payload: ComputeSettingsIn = { sql_warehouse_id: resolvedWarehouse, jobs_compute };
    saveMutation.mutate(
      { data: payload },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetComputeSettingsQueryKey() });
          queryClient.invalidateQueries({ queryKey: getGetWarehouseAccessQueryKey({ warehouse_id: resolvedWarehouse }) });
          toast.success(t("config.computeSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.computeSaveFailed"));
        },
      },
    );
  }, [saveMutation, queryClient, t]); // currentRef is stable — no need in deps

  const handleWarehouseChange = (v: string) => {
    setWarehouseId(v);
    save({ warehouseId: v });
  };

  const handleJobsKindChange = (v: string) => {
    const kind = v as JobsComputeModel["kind"];
    setJobsKind(kind);
    save({ kind });
  };

  const handleClusterChange = (v: string) => {
    setClusterId(v);
    save({ clusterId: v });
  };

  const handleGrant = () => {
    if (!checkWarehouseId) return;
    grantMutation.mutate(
      { data: { warehouse_id: checkWarehouseId } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetWarehouseAccessQueryKey({ warehouse_id: checkWarehouseId }) });
          toast.success(t("config.computeGrantSucceeded"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.computeGrantFailed"));
        },
      },
    );
  };

  return (
    <CardContent className="space-y-4">
      {/* SQL warehouse */}
      <div className="flex items-center justify-between rounded-md border p-3">
        <div className="space-y-0.5 pr-4">
          <Label className="text-sm">{t("config.computeWarehouseLabel")}</Label>
          <p className="text-[11px] text-muted-foreground">{t("config.computeWarehouseHint")}</p>
        </div>
        <WarehouseSelect
          value={warehouseId}
          onChange={handleWarehouseChange}
          warehouses={warehouses}
          disabled={!isAdmin || saveMutation.isPending}
        />
      </div>

      {/* SP access warning */}
      {access?.status === "missing" && checkWarehouseId && (
        <div className="flex items-start gap-2 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-700 dark:bg-amber-950/30 dark:text-amber-300">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400 mt-0.5" />
          <div className="space-y-1.5">
            <p>{t("config.computeSpAccessMissing")}</p>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="inline-block">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={handleGrant}
                      disabled={!isAdmin || grantMutation.isPending}
                      className="gap-1.5 h-7 text-xs border-amber-400 text-amber-700 hover:bg-amber-100 dark:text-amber-300 dark:hover:bg-amber-900"
                    >
                      {grantMutation.isPending ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <ShieldCheck className="h-3.5 w-3.5" />
                      )}
                      {t("config.computeGrantButton")}
                    </Button>
                  </span>
                </TooltipTrigger>
                {!isAdmin && <TooltipContent>{t("config.computeGrantAdminOnly")}</TooltipContent>}
              </Tooltip>
            </TooltipProvider>
          </div>
        </div>
      )}

      {/* Jobs compute */}
      <div className="flex items-start justify-between rounded-md border p-3">
        <div className="space-y-0.5 pr-4">
          <Label className="text-sm">{t("config.computeJobsLabel")}</Label>
          <p className="text-[11px] text-muted-foreground">{t("config.computeJobsHint")}</p>
        </div>
        <div className="space-y-1.5">
          <Select
            value={jobsKind}
            onValueChange={handleJobsKindChange}
            disabled={!isAdmin || saveMutation.isPending}
          >
            <SelectTrigger className="h-8 text-xs w-52">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="serverless" className="text-xs">
                <span className="flex items-center gap-1.5">
                  <span className="h-1.5 w-1.5 rounded-full bg-green-500 shrink-0" />
                  {t("config.computeJobsServerless")}
                </span>
              </SelectItem>
              <SelectItem value="existing_cluster" className="text-xs">
                <span className="flex items-center gap-1.5">
                  <span className="h-1.5 w-1.5 rounded-full border border-muted-foreground/50 shrink-0" />
                  {t("config.computeJobsCluster")}
                </span>
              </SelectItem>
            </SelectContent>
          </Select>
          {jobsKind === "existing_cluster" && (
            <>
              <ClusterSelect
                value={clusterId}
                onChange={handleClusterChange}
                clusters={clusters}
                disabled={!isAdmin || saveMutation.isPending}
              />
              <p className="text-[11px] text-muted-foreground leading-relaxed pt-1">
                {t("config.computeJobsClusterNote")}
              </p>
            </>
          )}
        </div>
      </div>

      {!isAdmin && <span className="text-xs text-muted-foreground">{t("config.computeAdminOnlyHint")}</span>}
    </CardContent>
  );
}

function ComputeSettingsCard() {
  const { t } = useTranslation();
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Cpu className="h-5 w-5" />
          {t("config.computeTitle")}
        </CardTitle>
      </CardHeader>
      <ComputeSettingsContent />
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Danger zone — the admin "Reset database" action. DESTRUCTIVE: wipes all
// DQX Studio-managed data. Guardrails are the point:
//   * The card + button are admin-only (the backend route is *also* hard-gated
//     to ADMIN; the UI gate is convenience, not the boundary).
//   * The final confirm button stays disabled until the admin types the exact
//     phrase, which is then sent to the backend as a confirmation token (the
//     server rejects a mismatch with 400 — defense-in-depth).
// The phrase MUST match RESET_CONFIRMATION_PHRASE in
// backend/services/database_reset_service.py — keep the two in lock-step.
// ─────────────────────────────────────────────────────────────────────────────

const RESET_DB_PHRASE = "reset dqx studio";

function DangerZoneCard() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { isAdmin } = usePermissions();
  const [open, setOpen] = useState(false);
  const [typed, setTyped] = useState("");
  const resetMutation = useResetDatabase();

  const canConfirm = typed.trim() === RESET_DB_PHRASE && !resetMutation.isPending;

  const closeDialog = () => {
    setOpen(false);
    setTyped("");
  };

  const handleConfirm = () => {
    if (!canConfirm) return;
    resetMutation.mutate(
      { data: { confirmation_phrase: RESET_DB_PHRASE } },
      {
        onSuccess: (resp) => {
          const cleared = resp.data.cleared_tables?.length ?? 0;
          const failed = Object.keys(resp.data.failed_tables ?? {}).length;
          if (failed > 0) {
            toast.warning(t("config.resetDbPartial", { count: failed }));
          } else {
            toast.success(t("config.resetDbSuccess", { count: cleared }));
          }
          closeDialog();
          // Everything the app cached is now stale — refetch across the board.
          queryClient.invalidateQueries();
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.resetDbFailed"));
        },
      },
    );
  };

  // B2-121: dark mode dulls `--destructive` (a dark, muted red) so the Danger
  // Zone barely reads. Brighten the red in dark mode only via Tailwind `dark:`
  // red-palette variants (text→red-400, borders→red-500, button→solid red-600)
  // — light mode keeps the shared destructive tokens untouched.
  return (
    <Card className="border-destructive/50 dark:border-red-500/60">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-destructive dark:text-red-400">
          <AlertTriangle className="h-5 w-5" />
          {t("config.resetDbTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-4 dark:border-red-500/50 dark:bg-red-950/40">
          <p className="flex items-center gap-2 text-sm font-semibold text-destructive dark:text-red-400">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            {t("config.resetDbWarningHeading")}
          </p>
          <p className="text-xs leading-relaxed text-muted-foreground dark:text-red-100/80">{t("config.resetDbWarningBody")}</p>
        </div>
        <div className="flex items-center gap-3">
          <Button
            variant="destructive"
            size="sm"
            disabled={!isAdmin}
            onClick={() => {
              setTyped("");
              setOpen(true);
            }}
            className="gap-1.5 dark:bg-red-600 dark:hover:bg-red-500"
          >
            <Trash2 className="h-3.5 w-3.5" />
            {t("config.resetDbButton")}
          </Button>
          {!isAdmin && <span className="text-xs text-muted-foreground">{t("config.resetDbAdminOnly")}</span>}
        </div>
      </CardContent>

      <Dialog
        open={open}
        onOpenChange={(o) => {
          if (resetMutation.isPending) return;
          if (o) setOpen(true);
          else closeDialog();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-destructive dark:text-red-400">
              <AlertTriangle className="h-5 w-5" />
              {t("config.resetDbDialogTitle")}
            </DialogTitle>
            <DialogDescription>{t("config.resetDbDialogBody")}</DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="reset-db-confirm" className="text-xs">
              {t("config.resetDbConfirmLabel", { phrase: RESET_DB_PHRASE })}
            </Label>
            <Input
              id="reset-db-confirm"
              value={typed}
              autoComplete="off"
              onChange={(e) => setTyped(e.target.value)}
              placeholder={t("config.resetDbConfirmPlaceholder")}
              disabled={resetMutation.isPending}
              onKeyDown={(e) => {
                if (e.key === "Enter" && canConfirm) handleConfirm();
              }}
            />
          </div>
          <DialogFooter>
            <Button variant="ghost" size="sm" onClick={closeDialog} disabled={resetMutation.isPending}>
              {t("config.resetDbCancel")}
            </Button>
            <Button variant="destructive" size="sm" onClick={handleConfirm} disabled={!canConfirm} className="gap-1.5 dark:bg-red-600 dark:hover:bg-red-500">
              {resetMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {resetMutation.isPending ? t("config.resetDbInProgress") : t("config.resetDbConfirmButton")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Deploy demo content — the admin action that seeds this workspace with a
// realistic sample DQX Studio deployment (sample tables, rules across every
// quality dimension, weeks of run history). NON-destructive by default, but the
// recommended path wipes existing Studio data first for a clean slate — so the
// wipe-first checkbox defaults to checked. The seed runs ~30min on a background
// thread; we poll the status endpoint (only while running) and surface a subtle
// banner. The backend route is hard-gated to ADMIN; the UI gate is convenience.
// ─────────────────────────────────────────────────────────────────────────────

function DeployDemoCard() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { isAdmin } = usePermissions();
  const [open, setOpen] = useState(false);
  const [wipeFirst, setWipeFirst] = useState(true);
  const deployMutation = useDeployDemoContent();

  // Poll the status endpoint continuously while a seed is running, then stop.
  const { data: statusResp } = useDemoContentStatus({
    query: {
      refetchInterval: (query) => (query.state.data?.data?.state === "running" ? 10000 : false),
    },
  });
  const status = statusResp?.data;
  const isRunning = status?.state === "running";

  const closeDialog = () => {
    setOpen(false);
  };

  const handleConfirm = () => {
    if (deployMutation.isPending) return;
    deployMutation.mutate(
      { data: { wipe_first: wipeFirst } },
      {
        onSuccess: () => {
          toast.success(t("config.demoStarted"));
          closeDialog();
          // Kick off polling immediately by refetching the status query.
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
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <FlaskConical className="h-5 w-5" />
          {t("config.demoTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm leading-relaxed text-muted-foreground">{t("config.demoBody")}</p>
        {isRunning && (
          <div className="flex items-center gap-2 rounded-md border border-primary/30 bg-primary/5 px-3 py-2 text-xs text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" />
            <span>{t("config.demoRunningBanner", { phase: status?.phase ?? "" })}</span>
          </div>
        )}
        <div className="flex items-center gap-3">
          <Button
            size="sm"
            disabled={!isAdmin || isRunning}
            onClick={() => {
              setWipeFirst(true);
              setOpen(true);
            }}
            className="gap-1.5"
          >
            {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {isRunning ? t("config.demoInProgress") : t("config.demoButton")}
          </Button>
          {!isAdmin && <span className="text-xs text-muted-foreground">{t("config.demoAdminOnly")}</span>}
        </div>
      </CardContent>

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
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Settings page shell — dqlake-style tabs, one card per setting, plus a
// client-side search that filters cards across every tab by title/keyword.
// The individual setting cards above are unchanged; this only regroups them.
// ─────────────────────────────────────────────────────────────────────────────

type SettingsTabId = "general" | "ai" | "compute" | "entitlements" | "governance" | "tags" | "danger";

/** ErrorBoundary + Suspense wrapper shared by every setting card. */
function SettingSection({ reset, children }: { reset: () => void; children: ReactNode }) {
  return (
    <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
      <Suspense fallback={<Skeleton className="h-40 w-full" />}>{children}</Suspense>
    </ErrorBoundary>
  );
}

function ConfigPage() {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [activeTab, setActiveTab] = useState<SettingsTabId>("general");

  useEffect(() => {
    if (!isAdmin) {
      navigate({ to: "/rules/active", replace: true });
    }
  }, [isAdmin, navigate]);

  const tabs = useMemo<{ id: SettingsTabId; label: string; icon: ComponentType<{ className?: string }> }[]>(
    () => [
      { id: "general", label: t("config.tabGeneral"), icon: SlidersHorizontal },
      { id: "ai", label: t("config.tabAi"), icon: Sparkles },
      { id: "compute", label: t("config.tabCompute"), icon: Cpu },
      { id: "entitlements", label: t("config.tabEntitlements"), icon: KeyRound },
      { id: "governance", label: t("config.tabGovernance"), icon: Scale },
      { id: "tags", label: t("config.tabTags"), icon: Tags },
      { id: "danger", label: t("config.tabDanger"), icon: AlertTriangle },
    ],
    [t],
  );

  const entries = useMemo<
    { id: string; tab: SettingsTabId; title: string; keywords: string; render: () => ReactNode }[]
  >(
    () => [
      { id: "timezone", tab: "general", title: t("config.timezoneTitle"), keywords: t("config.kwTimezone"), render: () => <TimezoneSettings /> },
      { id: "reviewStatuses", tab: "governance", title: t("config.reviewStatusesTitle"), keywords: t("config.kwReviewStatuses"), render: () => <RunReviewStatusesSettings /> },
      { id: "globalResults", tab: "general", title: t("config.globalResultsTitle"), keywords: t("config.kwGlobalResults"), render: () => <GlobalResultsSettingsCard /> },
      { id: "ai", tab: "ai", title: t("config.aiSettingsTitle"), keywords: t("config.kwAi"), render: () => <AiSettingsCard /> },
      { id: "labels", tab: "tags", title: t("config.labelsTitle"), keywords: t("config.kwLabels"), render: () => <LabelDefinitionsSettings /> },
      { id: "rulesRegistry", tab: "governance", title: t("config.rulesRegistrySettingsTitle"), keywords: t("config.kwRulesRegistry"), render: () => <RulesRegistrySettingsCard /> },
      { id: "approvalsMode", tab: "governance", title: t("config.approvalsModeTitle"), keywords: t("config.kwApprovalsMode"), render: () => <ApprovalsModeCard /> },
      { id: "requireDraftRun", tab: "governance", title: t("config.requireDraftRunTitle"), keywords: t("config.kwRequireDraftRun"), render: () => <RequireDraftRunSettingsCard /> },
      { id: "retention", tab: "governance", title: t("config.retentionTitle"), keywords: t("config.kwRetention"), render: () => <RetentionSettings /> },
      { id: "entitlements", tab: "entitlements", title: t("roleManagement.title"), keywords: t("config.kwEntitlements"), render: () => <RoleManagement /> },
      { id: "permissions", tab: "entitlements", title: t("config.permissionsDefaultInheritTitle"), keywords: t("config.kwPermissions"), render: () => <PermissionsSettingsCard /> },
      { id: "compute", tab: "compute", title: t("config.computeTitle"), keywords: t("config.kwCompute"), render: () => <ComputeSettingsCard /> },
      { id: "draftSample", tab: "compute", title: t("config.draftSampleTitle"), keywords: t("config.kwDraftSample"), render: () => <DraftRunSampleLimitSettings /> },
      { id: "deployDemo", tab: "danger", title: t("config.demoTitle"), keywords: t("config.kwDemo"), render: () => <DeployDemoCard /> },
      { id: "resetDatabase", tab: "danger", title: t("config.resetDbTitle"), keywords: t("config.kwDanger"), render: () => <DangerZoneCard /> },
    ],
    [t],
  );

  if (!isAdmin) {
    return null;
  }

  const query = search.trim().toLowerCase();
  const matches = query
    ? entries.filter(
        (e) => e.title.toLowerCase().includes(query) || e.keywords.toLowerCase().includes(query),
      )
    : [];

  return (
    <FadeIn>
      <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb page={t("config.breadcrumb")} />
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">
              <ShinyText text={t("config.title")} speed={6} className="font-bold" />
            </h1>
            <p className="text-muted-foreground">{t("config.subtitle")}</p>
          </div>
          <div className="relative w-full sm:w-72">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder={t("config.searchPlaceholder")}
              className="h-9 pl-9"
              aria-label={t("config.searchPlaceholder")}
            />
            {search && (
              <button
                type="button"
                onClick={() => setSearch("")}
                aria-label={t("common.close")}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                <X className="h-4 w-4" />
              </button>
            )}
          </div>
        </div>
      </div>

      <QueryErrorResetBoundary>
        {({ reset }) =>
          query ? (
            matches.length > 0 ? (
              <div className="space-y-6 pb-8">
                {matches.map((e) => (
                  <FadeIn key={e.id}>
                    <SettingSection reset={reset}>{e.render()}</SettingSection>
                  </FadeIn>
                ))}
              </div>
            ) : (
              <div className="rounded-md border border-dashed p-10 text-center text-sm text-muted-foreground">
                {t("config.searchNoResults", { query: search.trim() })}
              </div>
            )
          ) : (
            <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as SettingsTabId)}>
              {/* Same segmented tab picker used by the Rules Registry /
                  Monitored Tables / Table Spaces tab shells (see
                  MonitoredTableTabsShell / ProductTabsShell): the shadcn
                  TabsList pill track (bg-muted, h-auto p-1) with the default
                  raised active-tab styling and gap-1.5 icon triggers. */}
              <TabsList className="inline-flex h-auto items-center p-1">
                {tabs.map((tab) => {
                  const Icon = tab.icon;
                  return (
                    <TabsTrigger
                      key={tab.id}
                      value={tab.id}
                      className={cn(
                        "gap-1.5",
                        // Danger Zone reads as destructive: label + icon stay red in
                        // every state (inactive/active, light/dark). tailwind-merge
                        // lets these override the trigger's default text tokens.
                        tab.id === "danger" &&
                          "text-destructive dark:text-red-400 data-[state=active]:text-destructive dark:data-[state=active]:text-red-400",
                      )}
                    >
                      <Icon
                        className={cn(
                          "h-4 w-4 shrink-0",
                          tab.id === "ai" && AI_ICON_COLOR,
                        )}
                      />
                      {tab.id === "ai" ? (
                        <span className={AI_TEXT_GRADIENT}>{tab.label}</span>
                      ) : (
                        tab.label
                      )}
                    </TabsTrigger>
                  );
                })}
              </TabsList>
              {tabs.map((tab) => (
                <TabsContent key={tab.id} value={tab.id} className="mt-4 space-y-6 pb-8">
                  {entries
                    .filter((e) => e.tab === tab.id)
                    // Cards within each tab are ordered alphabetically by title.
                    .sort((a, b) => a.title.localeCompare(b.title))
                    .map((e) => (
                      <FadeIn key={e.id}>
                        <SettingSection reset={reset}>{e.render()}</SettingSection>
                      </FadeIn>
                    ))}
                </TabsContent>
              ))}
            </Tabs>
          )
        }
      </QueryErrorResetBoundary>
      </div>
    </FadeIn>
  );
}
