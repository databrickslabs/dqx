import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { AlertCircle, AlertTriangle, CheckCircle2, Circle, Clock, Cpu, Database, Globe, KeyRound, LayoutDashboard, Loader2, Lock, Search, Tags, Plus, Trash2, X, ExternalLink, RotateCcw, ShieldCheck, Sparkles } from "lucide-react";
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
import { Switch } from "@/components/ui/switch";
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
  useEmbeddedDashboard,
  useSaveEmbeddedDashboard,
  useDeleteEmbeddedDashboard,
  getEmbeddedDashboardQueryKey,
  useRunReviewStatuses,
  useSaveRunReviewStatuses,
  getRunReviewStatusesQueryKey,
  type LabelDefinition,
  type RetentionSettingsOut,
  type RunReviewStatusOption,
} from "@/lib/api-custom";
import {
  useGetAiSettings,
  useSaveAiSettings,
  getGetAiSettingsQueryKey,
  useListServingEndpoints,
  useEnsureVectorStore,
  useGetRulesRegistrySettings,
  useSaveRulesRegistrySettings,
  getGetRulesRegistrySettingsQueryKey,
  useGetComputeSettings,
  useSaveComputeSettings,
  getGetComputeSettingsQueryKey,
  useGetPermissionsDefaultInherit,
  useSetPermissionsDefaultInherit,
  getGetPermissionsDefaultInheritQueryKey,
  useListComputeWarehouses,
  useListComputeClusters,
  useGetWarehouseAccess,
  getGetWarehouseAccessQueryKey,
  useGrantWarehouseAccess,
  type AiSettingsIn,
  type RulesRegistrySettingsIn,
  type ComputeSettingsIn,
  type JobsComputeModel,
} from "@/lib/api";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { ChevronDown, Check } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { AI_BUTTON_BG, AI_ICON_COLOR, AI_TEXT_GRADIENT } from "@/lib/ai-style";

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
const HEX_COLOR_RE = /^#[0-9A-Fa-f]{6}$/;
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

interface DraftDefinition extends LabelDefinition {
  draftId: string;
}

function defToDraft(d: LabelDefinition): DraftDefinition {
  return {
    draftId: crypto.randomUUID(),
    key: d.key,
    description: d.description ?? "",
    values: [...d.values],
    allow_custom_values: !!d.allow_custom_values,
    value_colors: d.value_colors ? { ...d.value_colors } : null,
    value_descriptions: d.value_descriptions ? { ...d.value_descriptions } : null,
    is_builtin: !!d.is_builtin,
  };
}

function draftToDef(d: DraftDefinition): LabelDefinition {
  const values = d.values.map((v) => v.trim()).filter(Boolean);
  const valueSet = new Set(values);
  const colors: Record<string, string> = {};
  for (const [value, color] of Object.entries(d.value_colors ?? {})) {
    if (valueSet.has(value) && HEX_COLOR_RE.test(color)) colors[value] = color;
  }
  const descriptions: Record<string, string> = {};
  for (const [value, desc] of Object.entries(d.value_descriptions ?? {})) {
    if (valueSet.has(value) && desc.trim()) descriptions[value] = desc;
  }
  return {
    key: d.key.trim(),
    description: (d.description ?? "").trim(),
    values,
    allow_custom_values: d.allow_custom_values,
    value_colors: Object.keys(colors).length > 0 ? colors : null,
    value_descriptions: Object.keys(descriptions).length > 0 ? descriptions : null,
    is_builtin: !!d.is_builtin,
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

  const addDraft = () =>
    setDrafts((prev) => [
      ...prev,
      {
        draftId: crypto.randomUUID(),
        key: "",
        description: "",
        values: [],
        allow_custom_values: false,
        value_colors: null,
        value_descriptions: null,
        is_builtin: false,
      },
    ]);

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
        </div>
      </CardContent>
    </Card>
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

  const patchValue = (i: number, patch: { name?: string; color?: string | null; description?: string }) => {
    const current = values[i];
    if (current === undefined) return;
    let nextValues = values;
    let nextColors = colors;
    let nextDescriptions = descriptions;

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

    onChange({
      values: nextValues,
      value_colors: Object.keys(nextColors).length > 0 ? nextColors : null,
      value_descriptions: Object.keys(nextDescriptions).length > 0 ? nextDescriptions : null,
    });
  };

  const removeAt = (i: number) => {
    const current = values[i];
    const nextColors = { ...colors };
    delete nextColors[current];
    const nextDescriptions = { ...descriptions };
    delete nextDescriptions[current];
    onChange({
      values: values.filter((_, idx) => idx !== i),
      value_colors: Object.keys(nextColors).length > 0 ? nextColors : null,
      value_descriptions: Object.keys(nextDescriptions).length > 0 ? nextDescriptions : null,
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
      {values.length === 0 && (
        <p className="text-[11px] italic text-muted-foreground py-1">{t("config.noValuesHint")}</p>
      )}
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
            <div className="grid grid-cols-[auto_1fr_auto] items-center gap-2 px-2.5 py-1.5">
              <ValueColorEditor value={v} color={color} onSetColor={(c) => patchValue(i, { color: c })} />
              <span className="flex items-baseline gap-2 min-w-0">
                <span className="font-mono text-xs shrink-0">{v}</span>
                {description && !isOpen && (
                  <span className="truncate text-[11px] text-muted-foreground">{description}</span>
                )}
              </span>
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
                  <div className="space-y-2 border-t px-2.5 pb-2.5 pt-2">
                    <div className="space-y-1">
                      <Label className="text-[10px] text-muted-foreground">
                        {t("config.valueLabel")} <RequiredAsterisk />
                      </Label>
                      <Input
                        value={v}
                        onChange={(e) => patchValue(i, { name: e.target.value })}
                        className="h-7 w-40 text-xs font-mono"
                      />
                    </div>
                    <div className="space-y-1">
                      <Label className="text-[10px] text-muted-foreground">{t("config.descriptionLabel")}</Label>
                      <Textarea
                        value={description}
                        onChange={(e) => patchValue(i, { description: e.target.value })}
                        placeholder={t("config.valueDescriptionPlaceholder")}
                        className="max-w-xs text-xs min-h-[28px] py-1 resize-none"
                        rows={1}
                      />
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        );
      })}
      <Button type="button" variant="outline" size="sm" onClick={addValue} className="h-7 gap-1.5 text-xs">
        <Plus className="h-3.5 w-3.5" />
        {t("config.addValue")}
      </Button>
    </div>
  );
}

function DefinitionEditorCard({ draft, onChange, onRemove }: DefinitionEditorCardProps) {
  const { t } = useTranslation();
  const keyValid = !draft.key || LABEL_KEY_RE.test(draft.key.trim());
  const isWeight = draft.key.trim() === RESERVED_WEIGHT_KEY;
  const isBuiltin = !!draft.is_builtin;
  const isLocked = isWeight || isBuiltin;
  return (
    <div className="rounded-md border bg-muted/30 p-3 space-y-3">
      {/* Key + description + delete sit in one 3-column grid (rather than a
          nested grid plus a flex sibling) so `items-end` aligns the delete
          button with the bottom of the input/textarea row instead of the
          top of the labels above them — keeps the bin icon inline with the
          row it acts on instead of floating above it. */}
      <div className="grid grid-cols-[180px_1fr_auto] gap-3 items-end">
        <div className="space-y-1">
          <Label className="text-xs flex items-center gap-1.5">
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
          {!keyValid && (
            <p className="text-[10px] text-destructive">
              {t("config.keyHint")}
            </p>
          )}
        </div>
        <div className="space-y-1">
          <Label className="text-xs">{t("config.descriptionLabel")}</Label>
          <Textarea
            value={draft.description ?? ""}
            onChange={(e) => onChange({ description: e.target.value })}
            placeholder={isWeight ? t("config.weightDescriptionPlaceholder") : t("config.descriptionPlaceholder")}
            disabled={isLocked}
            className="max-w-xs text-xs min-h-[32px] py-1.5 resize-none"
            rows={1}
          />
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-8 w-8 shrink-0 text-destructive hover:text-destructive disabled:opacity-30"
          onClick={onRemove}
          disabled={isBuiltin}
          title={isBuiltin ? t("config.builtinCannotDelete") : undefined}
          aria-label={t("config.removeDefinition")}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
      <div className="space-y-1.5">
        <div className="flex items-center justify-between">
          <Label className="text-xs">{t("config.allowedValues")}</Label>
          {/* Reserved keys (dimension/severity) always disallow custom
              values server-side (see `_NO_CUSTOM_VALUE_BUILTIN_KEYS` in
              routes.v1.config) — the checkbox stays visible so that's
              legible at a glance, just disabled/unchecked rather than
              hidden entirely. */}
          <label
            className={cn(
              "flex items-center gap-1.5 text-xs",
              isBuiltin ? "text-muted-foreground" : "cursor-pointer",
            )}
          >
            <Checkbox
              checked={isBuiltin ? false : draft.allow_custom_values}
              onCheckedChange={(c) => onChange({ allow_custom_values: c === true })}
              disabled={isBuiltin}
            />
            <span>{t("config.allowCustomValues")}</span>
          </label>
        </div>
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

function RetentionSettings() {
  const { t } = useTranslation();
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
          toast.success(t("config.retentionSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.failedSaveRetention"));
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

// ─────────────────────────────────────────────────────────────────────────────
// Embedded Dashboard — pins a Databricks AI/BI dashboard ID into app state so
// the Insights page can render it inside an iframe. Falls back to the env
// default (set by the bundle's DQX_DEFAULT_DASHBOARD_ID) when unset, so a
// shipped starter dashboard works out-of-the-box.
// ─────────────────────────────────────────────────────────────────────────────

function EmbeddedDashboardSettings() {
  const { t } = useTranslation();
  const { data, isLoading } = useEmbeddedDashboard();
  const queryClient = useQueryClient();
  const saveMutation = useSaveEmbeddedDashboard();
  const deleteMutation = useDeleteEmbeddedDashboard();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  const [dashboardId, setDashboardId] = useState<string>("");
  const [title, setTitle] = useState<string>("");
  const [hydrated, setHydrated] = useState(false);
  // Clearing the dashboard override affects every user immediately —
  // gate it behind a confirm dialog so a stray click doesn't blow away
  // a pinned dashboard.
  const [confirmClearOpen, setConfirmClearOpen] = useState(false);

  useEffect(() => {
    if (data && !hydrated) {
      // Only seed the inputs with admin-saved values. If only the env
      // default is in play, leave the inputs blank so the placeholder
      // copy makes clear the field is empty (and saving an empty value
      // would be rejected).
      if (data.is_set) {
        setDashboardId(data.dashboard_id);
        setTitle(data.title ?? "");
      }
      setHydrated(true);
    }
  }, [data, hydrated]);

  const trimmedId = dashboardId.trim();
  const trimmedTitle = title.trim();

  const isDirty = useMemo(() => {
    if (!data) return false;
    if (!data.is_set) return trimmedId !== "";
    return trimmedId !== data.dashboard_id || trimmedTitle !== (data.title ?? "");
  }, [data, trimmedId, trimmedTitle]);

  const validationError = useMemo(() => {
    if (!trimmedId) return null;
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(trimmedId)) {
      return "Use the ID only (letters/digits/_/-, ≤128 chars) — not a full URL.";
    }
    return null;
  }, [trimmedId]);

  const previewUrl = useMemo(() => {
    if (!data?.workspace_host || !trimmedId || validationError) return null;
    return `${data.workspace_host}/dashboardsv3/${trimmedId}`;
  }, [data?.workspace_host, trimmedId, validationError]);

  const handleSave = () => {
    if (!trimmedId || validationError) return;
    saveMutation.mutate(
      { data: { dashboard_id: trimmedId, title: trimmedTitle || null } },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getEmbeddedDashboardQueryKey() });
          toast.success(t("config.dashboardSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.failedSaveDashboard"));
        },
      },
    );
  };

  const handleClear = () => {
    setConfirmClearOpen(true);
  };

  const confirmClear = () => {
    setConfirmClearOpen(false);
    deleteMutation.mutate(undefined, {
      onSuccess: () => {
        setDashboardId("");
        setTitle("");
        setHydrated(false);
        queryClient.invalidateQueries({ queryKey: getEmbeddedDashboardQueryKey() });
        toast.success(t("config.clearedDashboardOverride"));
      },
      onError: () => toast.error(t("config.failedClearDashboardOverride")),
    });
  };

  if (isLoading || !data) return <Skeleton className="h-40 w-full" />;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <LayoutDashboard className="h-5 w-5" />
          Insights dashboard
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          Pin a Databricks AI/BI dashboard to the <strong className="text-foreground">Insights</strong> page.
          Anyone with access to this app sees the dashboard rendered as an iframe; row-level visibility
          is enforced by Unity Catalog on the underlying tables. Build your dashboard against{" "}
          <code>dq_validation_runs</code>, <code>dq_metrics</code>, <code>dq_quarantine_records</code>, and{" "}
          <code>dq_profiling_results</code>, then paste the ID below.
        </p>

        {data.is_default && !data.is_set && (
          <div className="rounded-md border border-blue-200/60 bg-blue-50/30 p-3 text-xs text-blue-900">
            A default dashboard is configured by the deployment bundle. Saving below overrides it
            for this workspace; "Restore default" reverts.
          </div>
        )}

        <div className="grid grid-cols-1 sm:grid-cols-[1fr_300px] gap-3">
          <div className="space-y-1.5">
            <Label htmlFor="embedded-dashboard-id" className="text-xs">
              Dashboard ID
            </Label>
            <Input
              id="embedded-dashboard-id"
              value={dashboardId}
              onChange={(e) => setDashboardId(e.target.value)}
              placeholder={
                data.is_default
                  ? `e.g. ${data.dashboard_id} (default)`
                  : "e.g. 01abc23d456789..."
              }
              disabled={!isAdmin || saveMutation.isPending || deleteMutation.isPending}
              className={cn("h-8 font-mono text-xs", validationError && "border-destructive")}
              autoComplete="off"
            />
            {validationError && (
              <p className="text-[11px] text-destructive flex items-center gap-1">
                <AlertCircle className="h-3 w-3" />
                {validationError}
              </p>
            )}
            <p className="text-[11px] text-muted-foreground">
              Find the ID in the dashboard URL after <code>/dashboardsv3/</code>.
            </p>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="embedded-dashboard-title" className="text-xs">
              Display title <span className="text-muted-foreground">(optional)</span>
            </Label>
            <Input
              id="embedded-dashboard-title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="e.g. Quality Overview"
              maxLength={200}
              disabled={!isAdmin || saveMutation.isPending || deleteMutation.isPending}
              className="h-8 text-xs"
            />
            <p className="text-[11px] text-muted-foreground">Shown on the Insights page header.</p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2 pt-2 border-t">
          <Button
            size="sm"
            onClick={handleSave}
            disabled={
              !isAdmin ||
              !isDirty ||
              !!validationError ||
              !trimmedId ||
              saveMutation.isPending ||
              deleteMutation.isPending
            }
          >
            {saveMutation.isPending && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            Save changes
          </Button>
          {data.is_set && (
            <Button
              size="sm"
              variant="ghost"
              onClick={handleClear}
              disabled={!isAdmin || saveMutation.isPending || deleteMutation.isPending}
              title={
                data.is_default
                  ? "Clear the workspace override and fall back to the default shipped by the bundle"
                  : "Clear the saved dashboard ID — the Insights page will show an empty state"
              }
              className="gap-1.5"
            >
              <RotateCcw className="h-3.5 w-3.5" />
              {data.is_default ? "Restore default" : "Clear"}
            </Button>
          )}
          {previewUrl && (
            <Button
              size="sm"
              variant="ghost"
              asChild
              className="gap-1.5 text-xs text-muted-foreground"
              title="Open the dashboard in a new tab to verify the ID is correct and you have access"
            >
              <a href={previewUrl} target="_blank" rel="noopener noreferrer">
                <ExternalLink className="h-3.5 w-3.5" />
                Preview in Databricks
              </a>
            </Button>
          )}
          {!isAdmin && (
            <span className="text-xs text-muted-foreground">Only admins can change this setting</span>
          )}
        </div>
      </CardContent>

      <AlertDialog open={confirmClearOpen} onOpenChange={setConfirmClearOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {data?.is_default ? "Restore default dashboard?" : "Clear the dashboard override?"}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {data?.is_default
                ? "This removes the workspace-level override. The Insights page will fall back to the default dashboard shipped by the deployment bundle."
                : "This clears the saved dashboard ID for every user of this app. The Insights page will show an empty state until a new dashboard is pinned."}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmClear}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {data?.is_default ? "Restore default" : "Clear"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
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
const REVIEW_STATUS_COLOR_TOKENS = ["gray", "amber", "green", "blue", "red", "purple"] as const;
type ReviewStatusColorToken = (typeof REVIEW_STATUS_COLOR_TOKENS)[number];

const REVIEW_STATUS_COLOR_CLASSES: Record<ReviewStatusColorToken, { swatch: string; badge: string }> = {
  gray: {
    swatch: "bg-gray-300 dark:bg-gray-700 border-gray-400 dark:border-gray-600",
    badge: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200 border-gray-300 dark:border-gray-700",
  },
  amber: {
    swatch: "bg-amber-400 border-amber-500",
    badge: "bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200 border-amber-300 dark:border-amber-800",
  },
  green: {
    swatch: "bg-green-500 border-green-600",
    badge: "bg-green-100 text-green-900 dark:bg-green-950 dark:text-green-200 border-green-300 dark:border-green-800",
  },
  blue: {
    swatch: "bg-blue-500 border-blue-600",
    badge: "bg-blue-100 text-blue-900 dark:bg-blue-950 dark:text-blue-200 border-blue-300 dark:border-blue-800",
  },
  red: {
    swatch: "bg-red-500 border-red-600",
    badge: "bg-red-100 text-red-900 dark:bg-red-950 dark:text-red-200 border-red-300 dark:border-red-800",
  },
  purple: {
    swatch: "bg-purple-500 border-purple-600",
    badge: "bg-purple-100 text-purple-900 dark:bg-purple-950 dark:text-purple-200 border-purple-300 dark:border-purple-800",
  },
};

/** Normalise an arbitrary color string to a known token; unknown values fall back to gray. */
function normaliseReviewStatusColor(value: string | undefined | null): ReviewStatusColorToken {
  const lower = (value || "").trim().toLowerCase();
  return (REVIEW_STATUS_COLOR_TOKENS as readonly string[]).includes(lower)
    ? (lower as ReviewStatusColorToken)
    : "gray";
}

function ReviewStatusColorSwatch({ color }: { color: string }) {
  const token = normaliseReviewStatusColor(color);
  return (
    <span
      className={cn(
        "inline-block h-3.5 w-3.5 rounded-full border",
        REVIEW_STATUS_COLOR_CLASSES[token].swatch,
      )}
      aria-hidden
    />
  );
}

// Exported helpers — re-used by the Runs detail dropdown and the
// Runs History badge so all three places render consistent colors
// without each one duplicating the token table.
export function reviewStatusBadgeClasses(color: string) {
  return REVIEW_STATUS_COLOR_CLASSES[normaliseReviewStatusColor(color)].badge;
}
export { REVIEW_STATUS_COLOR_TOKENS };

function RunReviewStatusesSettings() {
  const { t } = useTranslation();
  const { data, isLoading } = useRunReviewStatuses();
  const queryClient = useQueryClient();
  const saveMutation = useSaveRunReviewStatuses();
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  // Local working copy; never mutated in-place. We re-hydrate from
  // the server response on first load and after every successful save
  // so concurrent edits from another admin don't get clobbered if the
  // user navigates away and back.
  const [draft, setDraft] = useState<RunReviewStatusOption[]>([]);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (data && !hydrated) {
      setDraft(data.statuses.map((s) => ({ ...s })));
      setHydrated(true);
    }
  }, [data, hydrated]);

  // Re-derive whether the form has unsaved changes from the canonical
  // server response rather than tracking a separate dirty flag —
  // cheaper and avoids drift after a partial save.
  const isDirty = useMemo(() => {
    if (!data) return false;
    if (data.statuses.length !== draft.length) return true;
    return data.statuses.some((s, i) => {
      const d = draft[i];
      return (
        !d ||
        d.value !== s.value ||
        d.description !== s.description ||
        d.color !== s.color ||
        d.is_default !== s.is_default
      );
    });
  }, [data, draft]);

  // The save endpoint enforces exactly-one-default; mirror that in the
  // UI so the button stays disabled when the constraint can't be met.
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
    if (defaults === 0) return "Pick one status as the default for unreviewed runs.";
    if (defaults > 1) return "Only one status can be marked default.";
    return null;
  }, [draft]);

  const handleAdd = () => {
    setDraft((d) => [
      ...d,
      { value: "", description: "", color: "gray", is_default: false },
    ]);
  };

  const handleRemove = (idx: number) => {
    setDraft((d) => d.filter((_, i) => i !== idx));
  };

  const handlePatch = (idx: number, patch: Partial<RunReviewStatusOption>) => {
    setDraft((d) => d.map((entry, i) => (i === idx ? { ...entry, ...patch } : entry)));
  };

  // Radio-group semantics on top of an array — selecting a default
  // unsets the previous one rather than allowing the constraint
  // violation to slip into the validation message.
  const handleMakeDefault = (idx: number) => {
    setDraft((d) => d.map((entry, i) => ({ ...entry, is_default: i === idx })));
  };

  const handleSave = () => {
    if (validationError) return;
    const cleaned = draft.map((entry) => ({
      value: entry.value.trim(),
      description: (entry.description || "").trim(),
      color: normaliseReviewStatusColor(entry.color),
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
  };

  const handleReset = () => {
    if (data) setDraft(data.statuses.map((s) => ({ ...s })));
  };

  if (isLoading || !data) {
    return <Skeleton className="h-40 w-full" />;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldCheck className="h-5 w-5" />
          Run review statuses
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          Reviewers tag each validation run with one of these values on the{" "}
          <strong className="text-foreground">Runs detail</strong> page (next to comments).
          The dropdown is filterable on the <strong className="text-foreground">Runs History</strong>{" "}
          page so the team can answer questions like "what's been acknowledged?" at a glance.
          One value is the <em>default</em> — newly completed runs surface that value until a reviewer
          changes it, so dashboards never see an empty state.
        </p>

        <div className="space-y-2">
          {draft.map((entry, idx) => {
            const colorToken = normaliseReviewStatusColor(entry.color);
            return (
              <div
                key={idx}
                className={cn(
                  "rounded-md border p-3 space-y-2",
                  entry.is_default && "border-primary/40 bg-primary/5",
                )}
              >
                <div className="grid grid-cols-1 sm:grid-cols-[1fr_2fr_auto_auto] gap-2 items-start">
                  <div className="space-y-1">
                    <Label className="text-[11px] text-muted-foreground">Value</Label>
                    <Input
                      value={entry.value}
                      onChange={(e) => handlePatch(idx, { value: e.target.value })}
                      placeholder="e.g. Acknowledged"
                      maxLength={80}
                      disabled={!isAdmin || saveMutation.isPending}
                      className="h-8 text-xs"
                      autoComplete="off"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-[11px] text-muted-foreground">Description (optional)</Label>
                    <Input
                      value={entry.description}
                      onChange={(e) => handlePatch(idx, { description: e.target.value })}
                      placeholder="Shown as a tooltip on the dropdown"
                      maxLength={200}
                      disabled={!isAdmin || saveMutation.isPending}
                      className="h-8 text-xs"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-[11px] text-muted-foreground">Color</Label>
                    <Popover>
                      <PopoverTrigger asChild>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          disabled={!isAdmin || saveMutation.isPending}
                          className="h-8 gap-1.5 text-xs"
                        >
                          <ReviewStatusColorSwatch color={colorToken} />
                          <span className="capitalize">{colorToken}</span>
                          <ChevronDown className="h-3 w-3 opacity-60" />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent align="end" className="w-44 p-1">
                        <div className="space-y-0.5">
                          {REVIEW_STATUS_COLOR_TOKENS.map((tok) => (
                            <button
                              key={tok}
                              type="button"
                              className={cn(
                                "flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs hover:bg-muted",
                                colorToken === tok && "bg-muted font-medium",
                              )}
                              onClick={() => handlePatch(idx, { color: tok })}
                            >
                              <ReviewStatusColorSwatch color={tok} />
                              <span className="capitalize">{tok}</span>
                              {colorToken === tok && <Check className="h-3 w-3 ml-auto" />}
                            </button>
                          ))}
                        </div>
                      </PopoverContent>
                    </Popover>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-[11px] text-muted-foreground sm:opacity-0 sm:pointer-events-none">.</Label>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      onClick={() => handleRemove(idx)}
                      disabled={!isAdmin || saveMutation.isPending || draft.length <= 1}
                      title={draft.length <= 1 ? "At least one status is required" : "Remove this status"}
                      className="h-8 w-8 p-0 text-muted-foreground hover:text-destructive"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </div>
                <button
                  type="button"
                  className={cn(
                    "flex items-center gap-1.5 text-xs",
                    entry.is_default
                      ? "text-primary font-medium"
                      : "text-muted-foreground hover:text-foreground",
                  )}
                  onClick={() => handleMakeDefault(idx)}
                  disabled={!isAdmin || saveMutation.isPending || entry.is_default}
                  title={
                    entry.is_default
                      ? "Default for new runs"
                      : "Make this the default surfaced for unreviewed runs"
                  }
                >
                  {entry.is_default ? (
                    <>
                      <CheckCircle2 className="h-3.5 w-3.5" /> Default for new runs
                    </>
                  ) : (
                    <>
                      <Circle className="h-3.5 w-3.5" /> Make default
                    </>
                  )}
                </button>
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
          className="gap-1.5"
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

        <div className="flex flex-wrap items-center gap-2 pt-2 border-t">
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!isAdmin || !isDirty || !!validationError || saveMutation.isPending}
          >
            {saveMutation.isPending && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            Save changes
          </Button>
          {isDirty && (
            <Button
              size="sm"
              variant="ghost"
              onClick={handleReset}
              disabled={!isAdmin || saveMutation.isPending}
              className="gap-1.5"
            >
              <RotateCcw className="h-3.5 w-3.5" />
              Reset
            </Button>
          )}
          {!isAdmin && (
            <span className="text-xs text-muted-foreground">Only admins can change this setting</span>
          )}
        </div>

        <div className="rounded-md border border-muted-foreground/20 bg-muted/40 p-3 text-[11px] text-muted-foreground space-y-1">
          <p>
            <strong className="text-foreground">Renaming a value</strong> doesn't rewrite existing
            run history — historical entries keep the old text so the audit trail stays accurate.
            To retire a value cleanly, leave it in the list (not as default) until the affected
            runs age out.
          </p>
        </div>
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

// Sentinel for "no endpoint selected" — Radix Select rejects an empty-string
// item value.
const NO_ENDPOINT_VALUE = "__none__";

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

  const isDirty = useMemo(() => {
    if (!data) return false;
    return aiEnabled !== data.ai_enabled || aiEndpoint.trim() !== data.ai_endpoint_name;
  }, [data, aiEnabled, aiEndpoint]);

  const handleSave = () => {
    const payload: AiSettingsIn = {
      ai_enabled: aiEnabled,
      ai_endpoint_name: aiEndpoint.trim(),
    };
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
          if (aiEnabled) {
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

  const handleReset = () => {
    if (!data) return;
    setAiEnabled(data.ai_enabled);
    setAiEndpoint(data.ai_endpoint_name);
  };

  if (isLoading || !data) {
    return <Skeleton className="h-40 w-full" />;
  }

  return (
    <Card className="border-fuchsia-500/30 dark:border-fuchsia-400/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Sparkles className={cn("h-5 w-5", AI_ICON_COLOR)} />
          <span className={cn("font-bold", AI_TEXT_GRADIENT)}>{t("config.aiSettingsTitle")}</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">{t("config.aiSettingsDescription")}</p>

        <div className="flex items-center justify-between rounded-md border p-3">
          <div className="space-y-0.5 pr-4">
            <Label htmlFor="ai-settings-enabled" className="text-sm">{t("config.aiSettingsEnabledLabel")}</Label>
            <p className="text-[11px] text-muted-foreground">{t("config.aiSettingsEnabledHint")}</p>
          </div>
          <Switch
            id="ai-settings-enabled"
            checked={aiEnabled}
            onCheckedChange={setAiEnabled}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        <div className="space-y-1">
          <Label className="text-[11px] text-muted-foreground">{t("config.aiSettingsEndpointLabel")}</Label>
          <ServingEndpointSelect
            value={aiEndpoint}
            onChange={setAiEndpoint}
            endpoints={servingEndpoints}
            disabled={!isAdmin || saveMutation.isPending}
          />
        </div>

        <div className="flex flex-wrap items-center gap-2 pt-2 border-t">
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!isAdmin || !isDirty || saveMutation.isPending}
            className={AI_BUTTON_BG}
          >
            {saveMutation.isPending && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            {t("config.aiSettingsSaveButton")}
          </Button>
          {isDirty && (
            <Button size="sm" variant="ghost" onClick={handleReset} disabled={!isAdmin || saveMutation.isPending} className="gap-1.5">
              <RotateCcw className="h-3.5 w-3.5" />
              {t("common.reset")}
            </Button>
          )}
          {!isAdmin && <span className="text-xs text-muted-foreground">{t("config.aiSettingsAdminOnlyHint")}</span>}
        </div>
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
            <p className="text-[11px] text-muted-foreground">{t("config.defaultAutoUpgradeHint")}</p>
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
            <Label htmlFor="auto-upgrade-without-approval" className="text-sm">
              {t("config.autoUpgradeWithoutApprovalLabel")}
            </Label>
            <p className="text-[11px] text-muted-foreground">{t("config.autoUpgradeWithoutApprovalHint")}</p>
          </div>
          <Switch
            id="auto-upgrade-without-approval"
            checked={settings.auto_upgrade_without_approval}
            onCheckedChange={(checked) => save({ auto_upgrade_without_approval: checked })}
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
        <p className="text-xs text-muted-foreground leading-relaxed">
          {t("config.permissionsDefaultInheritHelp")}
        </p>

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

function ComputeSettingsCard() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { data: settingsResp, isLoading } = useGetComputeSettings();
  const settings = settingsResp?.data;
  const { data: role } = useCurrentUserRoleSuspense();
  const isAdmin = role?.data?.role === "admin";

  const saveMutation = useSaveComputeSettings();
  const grantMutation = useGrantWarehouseAccess();
  const { data: warehousesResp } = useListComputeWarehouses();
  const warehouses = useMemo(() => warehousesResp?.data ?? [], [warehousesResp]);
  const { data: clustersResp } = useListComputeClusters();
  const clusters = useMemo(() => clustersResp?.data ?? [], [clustersResp]);

  const [warehouseId, setWarehouseId] = useState("");
  const [jobsKind, setJobsKind] = useState<JobsComputeModel["kind"]>("serverless");
  const [clusterId, setClusterId] = useState("");
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => {
    if (settings && !hydrated) {
      setWarehouseId(settings.sql_warehouse_id ?? "");
      setJobsKind(settings.jobs_compute?.kind ?? "serverless");
      setClusterId(settings.jobs_compute?.cluster_id ?? "");
      setHydrated(true);
    }
  }, [settings, hydrated]);

  // The warehouse whose SP access we check: the picked one, else the effective
  // (env-fallback) warehouse. Re-runs whenever the pick changes.
  const checkWarehouseId = warehouseId || settings?.effective_warehouse_id || "";
  const { data: accessResp } = useGetWarehouseAccess(
    { warehouse_id: checkWarehouseId },
    { query: { enabled: !!checkWarehouseId } },
  );
  const access = accessResp?.data;

  const isDirty = useMemo(() => {
    if (!settings) return false;
    return (
      warehouseId !== (settings.sql_warehouse_id ?? "") ||
      jobsKind !== (settings.jobs_compute?.kind ?? "serverless") ||
      (jobsKind === "existing_cluster" && clusterId !== (settings.jobs_compute?.cluster_id ?? ""))
    );
  }, [settings, warehouseId, jobsKind, clusterId]);

  const handleSave = () => {
    const jobs_compute: JobsComputeModel =
      jobsKind === "existing_cluster"
        ? { kind: "existing_cluster", cluster_id: clusterId || null }
        : { kind: "serverless", cluster_id: null };
    const payload: ComputeSettingsIn = { sql_warehouse_id: warehouseId, jobs_compute };
    saveMutation.mutate(
      { data: payload },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({ queryKey: getGetComputeSettingsQueryKey() });
          queryClient.invalidateQueries({ queryKey: getGetWarehouseAccessQueryKey({ warehouse_id: checkWarehouseId }) });
          toast.success(t("config.computeSaved"));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("config.computeSaveFailed"));
        },
      },
    );
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

  if (isLoading || !settings) return <Skeleton className="h-40 w-full" />;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Cpu className="h-4 w-4" />
          {t("config.computeTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-5">
        <p className="text-xs text-muted-foreground leading-relaxed">{t("config.computeDescription")}</p>

        {/* SQL warehouse */}
        <div className="space-y-1.5">
          <Label className="text-[11px] text-muted-foreground flex items-center gap-1.5">
            <Database className="h-3.5 w-3.5" />
            {t("config.computeWarehouseLabel")}
          </Label>
          <Select
            value={warehouseId || NO_WAREHOUSE_VALUE}
            onValueChange={(v) => setWarehouseId(v === NO_WAREHOUSE_VALUE ? "" : v)}
            disabled={!isAdmin || saveMutation.isPending}
          >
            <SelectTrigger className="h-8 text-xs font-mono w-full">
              <SelectValue placeholder={t("config.computeWarehousePlaceholder")} />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={NO_WAREHOUSE_VALUE} className="text-xs">
                {t("config.computeWarehouseDefault")}
              </SelectItem>
              {warehouses.length === 0 && (
                <SelectLabel className="font-normal">{t("config.computeNoWarehouses")}</SelectLabel>
              )}
              {warehouses.map((w) => (
                <SelectItem key={w.id} value={w.id} className="text-xs font-mono">
                  {w.name}
                  {w.serverless ? " · serverless" : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
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
        <div className="space-y-1.5">
          <Label className="text-[11px] text-muted-foreground flex items-center gap-1.5">
            <Cpu className="h-3.5 w-3.5" />
            {t("config.computeJobsLabel")}
          </Label>
          <Select
            value={jobsKind}
            onValueChange={(v) => setJobsKind(v as JobsComputeModel["kind"])}
            disabled={!isAdmin || saveMutation.isPending}
          >
            <SelectTrigger className="h-8 text-xs w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="serverless" className="text-xs">
                {t("config.computeJobsServerless")}
              </SelectItem>
              <SelectItem value="existing_cluster" className="text-xs">
                {t("config.computeJobsCluster")}
              </SelectItem>
            </SelectContent>
          </Select>
          {jobsKind === "existing_cluster" && (
            <>
              <Select
                value={clusterId || NO_CLUSTER_VALUE}
                onValueChange={(v) => setClusterId(v === NO_CLUSTER_VALUE ? "" : v)}
                disabled={!isAdmin || saveMutation.isPending}
              >
                <SelectTrigger className="h-8 text-xs font-mono w-full mt-1.5">
                  <SelectValue placeholder={t("config.computeClusterPlaceholder")} />
                </SelectTrigger>
                <SelectContent>
                  {clusters.length === 0 && (
                    <SelectLabel className="font-normal">{t("config.computeNoClusters")}</SelectLabel>
                  )}
                  {clusters.map((c) => (
                    <SelectItem key={c.cluster_id} value={c.cluster_id} className="text-xs font-mono">
                      {c.cluster_name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-[11px] text-muted-foreground leading-relaxed pt-1">
                {t("config.computeJobsClusterNote")}
              </p>
            </>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2 pt-2 border-t">
          <Button size="sm" onClick={handleSave} disabled={!isAdmin || !isDirty || saveMutation.isPending}>
            {saveMutation.isPending && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            {t("config.computeSaveButton")}
          </Button>
          {!isAdmin && <span className="text-xs text-muted-foreground">{t("config.computeAdminOnlyHint")}</span>}
        </div>
      </CardContent>
    </Card>
  );
}

const NO_WAREHOUSE_VALUE = "__default__";
const NO_CLUSTER_VALUE = "__none__";

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
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <TimezoneSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.1}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <LabelDefinitionsSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.15}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <RunReviewStatusesSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.17}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <AiSettingsCard />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.18}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <RulesRegistrySettingsCard />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.185}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <PermissionsSettingsCard />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.19}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <ComputeSettingsCard />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.2}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <EmbeddedDashboardSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.25}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <Suspense fallback={<Skeleton className="h-40 w-full" />}>
                  <RetentionSettings />
                </Suspense>
              </ErrorBoundary>
            </FadeIn>
            <FadeIn delay={0.3}>
              <ErrorBoundary onReset={reset} FallbackComponent={SectionError}>
                <RoleManagement />
              </ErrorBoundary>
            </FadeIn>
          </div>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}
