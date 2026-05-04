import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { Button } from "@/components/ui/button";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { AlertCircle, Globe, Loader2, Search, Tags, Plus, Trash2, X } from "lucide-react";
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
  type LabelDefinition,
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
  return (
    <div className="flex flex-col gap-2 items-start">
      <p className="text-sm text-destructive flex items-center gap-1">
        <AlertCircle className="h-4 w-4" /> Failed to load section
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
        Retry
      </Button>
    </div>
  );
}

function TimezoneSettings() {
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
          toast.success(`Timezone updated to ${value}`);
        },
        onError: () => toast.error("Failed to save timezone"),
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
          Display Timezone
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
                  placeholder="Search timezones..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="h-8 border-0 p-0 shadow-none focus-visible:ring-0"
                />
              </div>
              <div className="max-h-72 overflow-y-auto">
                {filtered.length === 0 && (
                  <p className="px-3 py-4 text-sm text-muted-foreground text-center">No timezone found.</p>
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
            <span className="text-xs text-muted-foreground">Only admins can change this setting</span>
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
        errors.push("Every definition needs a key.");
        continue;
      }
      if (!LABEL_KEY_RE.test(k)) {
        errors.push(
          `Key "${k}" must start with a letter and contain only letters, digits, and underscores.`,
        );
      }
      if (seen.has(k)) errors.push(`Duplicate key "${k}".`);
      seen.add(k);
    }
    return errors;
  }, [drafts]);

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
              ? "Cleared rule labels."
              : `Saved ${definitions.length} rule label${definitions.length === 1 ? "" : "s"}.`,
          );
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? "Failed to save label definitions");
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
          Rule Labels
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {drafts.length === 0 && (
          <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
            No label definitions yet.
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
            Add label definition
          </Button>
          {!hasWeightKey && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => addDraft(RESERVED_WEIGHT_KEY)}
              className="gap-1.5 text-xs text-muted-foreground"
              title="Add a weight label with values 1..5 quickly"
            >
              <Plus className="h-3.5 w-3.5" />
              Add weight definition
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
            Save changes
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={handleReset}
            disabled={!isDirty || saveMutation.isPending}
          >
            Reset
          </Button>
          {!isDirty && (data?.definitions?.length ?? 0) > 0 && (
            <span className="text-xs text-muted-foreground">
              {data?.definitions?.length} definition
              {(data?.definitions?.length ?? 0) === 1 ? "" : "s"} active
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
  const keyValid = !draft.key || LABEL_KEY_RE.test(draft.key.trim());
  const isWeight = draft.key.trim() === RESERVED_WEIGHT_KEY;
  return (
    <div className={cn("rounded-md border p-3 space-y-3", isWeight ? "bg-blue-50/30 border-blue-200/60" : "bg-muted/20")}>
      <div className="flex items-start gap-3">
        <div className="grid grid-cols-[180px_1fr] gap-3 flex-1 items-start">
          <div className="space-y-1">
            <Label className="text-xs flex items-center gap-1.5">
              Key
              {isWeight && (
                <Badge variant="secondary" className="h-4 px-1 text-[10px] font-normal">
                  reserved
                </Badge>
              )}
            </Label>
            <Input
              value={draft.key}
              onChange={(e) => onChange({ key: e.target.value })}
              placeholder="e.g. team"
              className={cn("h-8 text-xs font-mono", !keyValid && "border-destructive")}
            />
            {!keyValid && (
              <p className="text-[10px] text-destructive">
                Letters, digits, underscore. Must start with a letter.
              </p>
            )}
            {isWeight && (
              <p className="text-[10px] text-blue-700">
                Drives the weight picker on rule authoring pages.
              </p>
            )}
          </div>
          <div className="space-y-1">
            <Label className="text-xs">
              Description <span className="text-muted-foreground">(optional)</span>
            </Label>
            <Textarea
              value={draft.description ?? ""}
              onChange={(e) => onChange({ description: e.target.value })}
              placeholder={isWeight ? "Rule weight (1 = informational, 5 = critical)" : "What this label captures (e.g. Owning team)"}
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
          aria-label="Remove definition"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
      <div className="space-y-1.5">
        <div className="flex items-center justify-between">
          <Label className="text-xs">
            Allowed values{" "}
            <span className="text-muted-foreground">
              (leave empty for a boolean tag)
            </span>
          </Label>
          <label className="flex items-center gap-1.5 text-xs cursor-pointer">
            <Checkbox
              checked={draft.allow_custom_values}
              onCheckedChange={(c) => onChange({ allow_custom_values: c === true })}
            />
            <span>Allow custom values</span>
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
                  aria-label={`Remove value ${v}`}
                >
                  <X className="h-3 w-3" />
                </button>
              </Badge>
            ))}
          </div>
        ) : (
          <p className="text-[11px] italic text-muted-foreground">
            No values — authors will toggle this label as a boolean tag (
            <code>true</code>/<code>false</code>).
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
            placeholder={isWeight ? "add weight value (e.g. 1)" : "add value… (press Enter)"}
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
            Add
          </Button>
        </div>
      </div>
    </div>
  );
}

function ConfigPage() {
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
        <PageBreadcrumb page="Configuration" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text="Configuration" speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            Manage roles, permissions, and display settings for your workspace.
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
                <RoleManagement />
              </ErrorBoundary>
            </FadeIn>
          </div>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}
