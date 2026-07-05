import {
  createFileRoute,
  Link,
  Navigate,
  useNavigate,
  useParams,
} from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import {
  useListRules,
  RunConfig,
  type RuleCatalogEntryOut,
} from "@/lib/api";
import axios, { isAxiosError } from "axios";
import {
  useBatchRunFromCatalog,
  getListValidationRunsQueryKey,
  notifyRuns,
} from "@/lib/api-custom";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Plus,
  Trash2,
  Save,
  AlertCircle,
  RotateCcw,
  Loader2,
  Play,
  History,
  CheckCircle2,
  XCircle,
  Clock,
  Search,
  CalendarClock,
  Layers,
  Database,
  Table2,
  Zap,
  ChevronRight,
  X,
  Link2,
  Copy,
} from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { useState, useEffect, useRef, Suspense, useMemo, useCallback } from "react";
import { useTranslation } from "react-i18next";
import yaml from "js-yaml";
import { useQueryClient, QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useAIAssistant } from "@/components/AIAssistantProvider";
import { FadeIn } from "@/components/anim/FadeIn";
import { useActiveRuns, type ActiveRun } from "@/hooks/use-active-runs";
import { getDryRunStatus, type RunStatusOut } from "@/lib/api";
import { cancelDryRun } from "@/lib/api-custom";
import { CircleStop, ShieldAlert } from "lucide-react";
import { parseFqn, formatDateTime as formatDate, getUserMetadata, labelToken, tokenToLabel } from "@/lib/format-utils";
import { LabelFilter, LabelsBadges, labelsMatchFilter } from "@/components/Labels";
import { usePermissions } from "@/hooks/use-permissions";
import { requireRunnerOrRedirect } from "@/lib/route-guards";

// Collect every distinct ``(key, value)`` label seen on the checks of the
// supplied approved rule sets. Used to populate ``<LabelFilter>`` dropdowns
// on the Execute tab and the schedule editor.
function collectAvailableLabels(
  rules: RuleCatalogEntryOut[],
): { key: string; value: string }[] {
  const seen = new Set<string>();
  const out: { key: string; value: string }[] = [];
  for (const r of rules) {
    for (const c of r.checks) {
      const md = getUserMetadata(c as Record<string, unknown>);
      for (const [k, v] of Object.entries(md)) {
        const tok = labelToken(k, v);
        if (!seen.has(tok)) {
          seen.add(tok);
          out.push({ key: k, value: v });
        }
      }
    }
  }
  return out;
}

// True iff at least one check on ``rule`` carries a label that satisfies
// the user's selection. Empty selection always passes.
function ruleMatchesLabels(
  rule: RuleCatalogEntryOut,
  selected: Set<string>,
): boolean {
  if (selected.size === 0) return true;
  return rule.checks.some((c) =>
    labelsMatchFilter(getUserMetadata(c as Record<string, unknown>), selected),
  );
}

// Merge every check's ``user_metadata`` for the rule set into a single
// ``Record<string, string>`` for display next to the table name.
// Most rule sets use the same labels across all checks; for the rare
// case where a key has different values per check, last-write-wins —
// the LabelFilter still shows every distinct ``(key, value)`` pair.
function collectRuleLabels(rule: RuleCatalogEntryOut): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const c of rule.checks) {
    const md = getUserMetadata(c as Record<string, unknown>);
    for (const [k, v] of Object.entries(md)) {
      merged[k] = v;
    }
  }
  return merged;
}

export const Route = createFileRoute("/_sidebar/runs")({
  // URL-level guard: aborts the route load before the page ever mounts
  // when the user lacks the RUNNER role. This complements the in-component
  // ``<Navigate>`` fallback below (kept as a defensive belt-and-suspenders
  // for any edge case where the component renders before the loader).
  beforeLoad: requireRunnerOrRedirect,
  component: RunsPage,
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const _SQL_CHECK_PREFIX = "__sql_check__/";
function cleanFqn(fqn: string) {
  return fqn.startsWith(_SQL_CHECK_PREFIX) ? fqn.slice(_SQL_CHECK_PREFIX.length) : fqn;
}

type GroupMode = "none" | "catalog" | "schema";

// ---------------------------------------------------------------------------
// Schedule config stored as JSON in checks_location
// ---------------------------------------------------------------------------

interface ScheduleConfig {
  frequency: "manual" | "hourly" | "daily" | "weekly" | "monthly" | "cron";
  cron_expression?: string;
  hour?: number;
  minute?: number;
  day_of_week?: number;
  day_of_month?: number;
  scope_mode: "all" | "catalog" | "schema" | "tables";
  scope_catalogs?: string[];
  scope_schemas?: string[];
  scope_tables?: string[];
  // Optional label filter intersected with the FQN-based scope above. A rule
  // set is included only if at least one of its checks carries a matching
  // ``user_metadata`` label. Persisted as a list of ``{key, value}`` so the
  // YAML round-trips cleanly and values containing ``=`` don't collide.
  scope_labels?: { key: string; value: string }[];
  sample_size?: number;
}

const DEFAULT_SCHEDULE: ScheduleConfig = {
  frequency: "daily",
  hour: 6,
  minute: 0,
  scope_mode: "all",
  sample_size: 1000,
};

// ---------------------------------------------------------------------------
// Schedule API (new per-row storage)
// ---------------------------------------------------------------------------

interface ScheduleConfigEntry {
  schedule_name: string;
  config: ScheduleConfig;
  version: number;
  created_by?: string | null;
  created_at?: string | null;
  updated_by?: string | null;
  updated_at?: string | null;
}

const SCHEDULES_KEY = ["/api/v1/schedules"] as const;

async function fetchSchedules(): Promise<ScheduleConfigEntry[]> {
  const resp = await axios.get<ScheduleConfigEntry[]>("/api/v1/schedules");
  return resp.data;
}

async function fetchSchedule(name: string): Promise<ScheduleConfigEntry> {
  const resp = await axios.get<ScheduleConfigEntry>(`/api/v1/schedules/${encodeURIComponent(name)}`);
  return resp.data;
}

async function saveScheduleApi(name: string, config: ScheduleConfig): Promise<ScheduleConfigEntry> {
  const resp = await axios.post<ScheduleConfigEntry>("/api/v1/schedules", {
    schedule_name: name,
    config,
  });
  return resp.data;
}

async function deleteScheduleApi(name: string): Promise<void> {
  await axios.delete(`/api/v1/schedules/${encodeURIComponent(name)}`);
}

type Translator = (key: string, options?: Record<string, unknown>) => string;

function cronPreview(cfg: ScheduleConfig, t: Translator): string {
  switch (cfg.frequency) {
    case "manual":
      return t("runs.cronManualOnly");
    case "hourly":
      return t("runs.cronHourly", { minute: String(cfg.minute ?? 0).padStart(2, "0") });
    case "daily":
      return t("runs.cronDaily", {
        hour: String(cfg.hour ?? 6).padStart(2, "0"),
        minute: String(cfg.minute ?? 0).padStart(2, "0"),
      });
    case "weekly": {
      const dayKeys = [
        "runs.dayShortSun",
        "runs.dayShortMon",
        "runs.dayShortTue",
        "runs.dayShortWed",
        "runs.dayShortThu",
        "runs.dayShortFri",
        "runs.dayShortSat",
      ];
      return t("runs.cronWeekly", {
        day: t(dayKeys[cfg.day_of_week ?? 1]),
        hour: String(cfg.hour ?? 6).padStart(2, "0"),
        minute: String(cfg.minute ?? 0).padStart(2, "0"),
      });
    }
    case "monthly":
      return t("runs.cronMonthly", {
        day: cfg.day_of_month ?? 1,
        hour: String(cfg.hour ?? 6).padStart(2, "0"),
        minute: String(cfg.minute ?? 0).padStart(2, "0"),
      });
    case "cron":
      return cfg.cron_expression
        ? t("runs.cronCustom", { expression: cfg.cron_expression })
        : t("runs.cronCustomNotSet");
    default:
      return "";
  }
}

// ---------------------------------------------------------------------------
// Schedule Frequency Picker
// ---------------------------------------------------------------------------

function ScheduleFrequencyPicker({
  config,
  onChange,
  disabled,
}: {
  config: ScheduleConfig;
  onChange: (cfg: ScheduleConfig) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const update = (patch: Partial<ScheduleConfig>) => onChange({ ...config, ...patch });

  const dayLongKeys = [
    "runs.daySunday",
    "runs.dayMonday",
    "runs.dayTuesday",
    "runs.dayWednesday",
    "runs.dayThursday",
    "runs.dayFriday",
    "runs.daySaturday",
  ];

  return (
    <div className="space-y-3">
      <div className="grid gap-2">
        <Label>{t("runs.frequency")}</Label>
        <Select value={config.frequency} onValueChange={(v) => update({ frequency: v as ScheduleConfig["frequency"] })} disabled={disabled}>
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="manual">{t("runs.freqManual")}</SelectItem>
            <SelectItem value="hourly">{t("runs.freqHourly")}</SelectItem>
            <SelectItem value="daily">{t("runs.freqDaily")}</SelectItem>
            <SelectItem value="weekly">{t("runs.freqWeekly")}</SelectItem>
            <SelectItem value="monthly">{t("runs.freqMonthly")}</SelectItem>
            <SelectItem value="cron">{t("runs.freqCron")}</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {config.frequency === "cron" && (
        <div className="grid gap-2">
          <Label>{t("runs.cronExpression")}</Label>
          <Input
            value={config.cron_expression || ""}
            onChange={(e) => update({ cron_expression: e.target.value })}
            disabled={disabled}
            placeholder={t("runs.cronPlaceholder")}
            className="font-mono text-sm"
          />
          <p className="text-xs text-muted-foreground">{t("runs.cronHint")}</p>
        </div>
      )}

      {config.frequency === "hourly" && (
        <div className="grid gap-2">
          <Label>{t("runs.minute")}</Label>
          <Select value={String(config.minute ?? 0)} onValueChange={(v) => update({ minute: Number(v) })} disabled={disabled}>
            <SelectTrigger className="w-24"><SelectValue /></SelectTrigger>
            <SelectContent>
              {Array.from({ length: 60 }, (_, i) => (
                <SelectItem key={i} value={String(i)}>:{String(i).padStart(2, "0")}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {(config.frequency === "daily" || config.frequency === "weekly" || config.frequency === "monthly") && (
        <div className="flex items-center gap-3">
          <div className="grid gap-2">
            <Label>{t("runs.hour")} <span className="text-muted-foreground font-normal">{t("runs.utcSuffix")}</span></Label>
            <Select value={String(config.hour ?? 6)} onValueChange={(v) => update({ hour: Number(v) })} disabled={disabled}>
              <SelectTrigger className="w-20"><SelectValue /></SelectTrigger>
              <SelectContent>
                {Array.from({ length: 24 }, (_, i) => (
                  <SelectItem key={i} value={String(i)}>{String(i).padStart(2, "0")}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid gap-2">
            <Label>{t("runs.minute")}</Label>
            <Select value={String(config.minute ?? 0)} onValueChange={(v) => update({ minute: Number(v) })} disabled={disabled}>
              <SelectTrigger className="w-20"><SelectValue /></SelectTrigger>
              <SelectContent>
                {Array.from({ length: 60 }, (_, i) => (
                  <SelectItem key={i} value={String(i)}>{String(i).padStart(2, "0")}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      )}

      {config.frequency === "weekly" && (
        <div className="grid gap-2">
          <Label>{t("runs.dayOfWeek")}</Label>
          <Select value={String(config.day_of_week ?? 1)} onValueChange={(v) => update({ day_of_week: Number(v) })} disabled={disabled}>
            <SelectTrigger className="w-32"><SelectValue /></SelectTrigger>
            <SelectContent>
              {dayLongKeys.map((dKey, i) => (
                <SelectItem key={i} value={String(i)}>{t(dKey)}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {config.frequency === "monthly" && (
        <div className="grid gap-2">
          <Label>{t("runs.dayOfMonth")}</Label>
          <Select value={String(config.day_of_month ?? 1)} onValueChange={(v) => update({ day_of_month: Number(v) })} disabled={disabled}>
            <SelectTrigger className="w-20"><SelectValue /></SelectTrigger>
            <SelectContent>
              {Array.from({ length: 28 }, (_, i) => (
                <SelectItem key={i + 1} value={String(i + 1)}>{i + 1}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {config.frequency !== "manual" && (
        <p className="text-xs text-muted-foreground flex items-center gap-1.5">
          <Clock className="h-3 w-3" /> {cronPreview(config, t)}
        </p>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Scope Selector — choose which approved rules to include
// ---------------------------------------------------------------------------

function ScopePicker({
  config,
  onChange,
  approvedRules,
  disabled,
}: {
  config: ScheduleConfig;
  onChange: (cfg: ScheduleConfig) => void;
  approvedRules: RuleCatalogEntryOut[];
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const update = (patch: Partial<ScheduleConfig>) => onChange({ ...config, ...patch });

  const allCatalogs = useMemo(() => {
    const set = new Set<string>();
    approvedRules.forEach((r) => { const c = parseFqn(r.table_fqn).catalog; if (c) set.add(c); });
    return Array.from(set).sort();
  }, [approvedRules]);

  const allSchemas = useMemo(() => {
    const set = new Set<string>();
    approvedRules.forEach((r) => {
      const { catalog, schema } = parseFqn(r.table_fqn);
      if (catalog && schema) set.add(`${catalog}.${schema}`);
    });
    return Array.from(set).sort();
  }, [approvedRules]);

  const availableLabels = useMemo(
    () => collectAvailableLabels(approvedRules),
    [approvedRules],
  );

  const selectedLabelTokens = useMemo(() => {
    const set = new Set<string>();
    for (const { key, value } of config.scope_labels ?? []) {
      set.add(labelToken(key, value));
    }
    return set;
  }, [config.scope_labels]);

  const onLabelFilterChange = (selected: Set<string>) => {
    const next = [...selected].map(tokenToLabel);
    update({ scope_labels: next.length > 0 ? next : undefined });
  };

  const matchedCount = useMemo(() => {
    return approvedRules.filter((r) => {
      const { catalog, schema } = parseFqn(r.table_fqn);
      const fqnMatches = (() => {
        switch (config.scope_mode) {
          case "all": return true;
          case "catalog": return (config.scope_catalogs ?? []).includes(catalog);
          case "schema": return (config.scope_schemas ?? []).includes(`${catalog}.${schema}`);
          case "tables": return (config.scope_tables ?? []).includes(r.table_fqn);
          default: return true;
        }
      })();
      if (!fqnMatches) return false;
      return ruleMatchesLabels(r, selectedLabelTokens);
    }).length;
  }, [approvedRules, config, selectedLabelTokens]);

  const toggleInList = (list: string[], item: string): string[] => {
    return list.includes(item) ? list.filter((x) => x !== item) : [...list, item];
  };

  return (
    <div className="space-y-3">
      <div className="grid gap-2">
        <Label>{t("runs.scope")}</Label>
        <Select value={config.scope_mode} onValueChange={(v) => update({ scope_mode: v as ScheduleConfig["scope_mode"] })} disabled={disabled}>
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all"><span className="flex items-center gap-1.5"><Database className="h-3 w-3" /> {t("runs.scopeAll")}</span></SelectItem>
            <SelectItem value="catalog"><span className="flex items-center gap-1.5"><Database className="h-3 w-3" /> {t("runs.scopeByCatalog")}</span></SelectItem>
            <SelectItem value="schema"><span className="flex items-center gap-1.5"><Layers className="h-3 w-3" /> {t("runs.scopeBySchema")}</span></SelectItem>
            <SelectItem value="tables"><span className="flex items-center gap-1.5"><Table2 className="h-3 w-3" /> {t("runs.scopeTables")}</span></SelectItem>
          </SelectContent>
        </Select>
      </div>

      {config.scope_mode === "catalog" && (
        <div className="border rounded-lg overflow-hidden max-h-44 overflow-y-auto">
          {allCatalogs.length === 0 ? (
            <p className="p-3 text-xs text-muted-foreground">{t("runs.noCatalogsInRules")}</p>
          ) : allCatalogs.map((cat) => (
            <div
              key={cat}
              className={cn("flex items-center gap-3 px-2.5 py-2 hover:bg-muted/20 cursor-pointer border-b last:border-b-0", (config.scope_catalogs ?? []).includes(cat) && "bg-primary/5")}
              onClick={() => !disabled && update({ scope_catalogs: toggleInList(config.scope_catalogs ?? [], cat) })}
            >
              <Checkbox checked={(config.scope_catalogs ?? []).includes(cat)} onCheckedChange={() => update({ scope_catalogs: toggleInList(config.scope_catalogs ?? [], cat) })} disabled={disabled} />
              <Database className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="text-sm">{cat}</span>
            </div>
          ))}
        </div>
      )}

      {config.scope_mode === "schema" && (
        <div className="border rounded-lg overflow-hidden max-h-44 overflow-y-auto">
          {allSchemas.length === 0 ? (
            <p className="p-3 text-xs text-muted-foreground">{t("runs.noSchemasInRules")}</p>
          ) : allSchemas.map((sch) => (
            <div
              key={sch}
              className={cn("flex items-center gap-3 px-2.5 py-2 hover:bg-muted/20 cursor-pointer border-b last:border-b-0", (config.scope_schemas ?? []).includes(sch) && "bg-primary/5")}
              onClick={() => !disabled && update({ scope_schemas: toggleInList(config.scope_schemas ?? [], sch) })}
            >
              <Checkbox checked={(config.scope_schemas ?? []).includes(sch)} onCheckedChange={() => update({ scope_schemas: toggleInList(config.scope_schemas ?? [], sch) })} disabled={disabled} />
              <Layers className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="font-mono text-xs">{sch}</span>
            </div>
          ))}
        </div>
      )}

      {config.scope_mode === "tables" && (
        <div className="border rounded-lg overflow-hidden max-h-56 overflow-y-auto">
          {approvedRules.length === 0 ? (
            <p className="p-3 text-xs text-muted-foreground">{t("runs.noApprovedRulesAvailable")}</p>
          ) : (
            <>
              <div className="flex items-center gap-3 p-2.5 bg-muted/40 border-b sticky top-0">
                <Checkbox
                  checked={approvedRules.length > 0 && approvedRules.every((r) => (config.scope_tables ?? []).includes(r.table_fqn))}
                  onCheckedChange={() => {
                    const allSelected = approvedRules.every((r) => (config.scope_tables ?? []).includes(r.table_fqn));
                    update({ scope_tables: allSelected ? [] : approvedRules.map((r) => r.table_fqn) });
                  }}
                  disabled={disabled}
                />
                <span className="text-xs font-medium text-muted-foreground">
                  {(config.scope_tables ?? []).length > 0
                    ? t("runs.selectedOf", { selected: (config.scope_tables ?? []).length, total: approvedRules.length })
                    : t("runs.selectAll")}
                </span>
              </div>
              {approvedRules.map((rule) => (
                <div
                  key={rule.table_fqn}
                  className={cn("flex items-center gap-3 px-2.5 py-2 hover:bg-muted/20 cursor-pointer border-b last:border-b-0", (config.scope_tables ?? []).includes(rule.table_fqn) && "bg-primary/5")}
                  onClick={() => !disabled && update({ scope_tables: toggleInList(config.scope_tables ?? [], rule.table_fqn) })}
                >
                  <Checkbox checked={(config.scope_tables ?? []).includes(rule.table_fqn)} onCheckedChange={() => update({ scope_tables: toggleInList(config.scope_tables ?? [], rule.table_fqn) })} disabled={disabled} />
                  <div className="flex items-center gap-2 flex-1 min-w-0 flex-wrap">
                    <span className="font-mono text-xs truncate">{rule.display_name || rule.table_fqn}</span>
                    <LabelsBadges labels={collectRuleLabels(rule)} max={3} size="xs" />
                  </div>
                  <Badge variant="secondary" className="text-[10px]">{rule.checks.length} rule{rule.checks.length !== 1 ? "s" : ""}</Badge>
                </div>
              ))}
            </>
          )}
        </div>
      )}

      <div className="grid gap-2">
        <Label>Filter by labels (optional)</Label>
        <div>
          <LabelFilter
            available={availableLabels}
            selected={selectedLabelTokens}
            onChange={onLabelFilterChange}
            className="h-9 w-full justify-between"
          />
        </div>
        {availableLabels.length === 0 && (
          <p className="text-[11px] text-muted-foreground">
            No labels found on approved rules — add labels to checks to use this filter.
          </p>
        )}
      </div>

      <p className="text-xs text-muted-foreground">
        {t("runs.matchedOfApproved", { matched: matchedCount, total: approvedRules.length, count: approvedRules.length })}
      </p>

      <div className="grid gap-2">
        <Label>{t("runs.rowScope")}</Label>
        <Select
          value={config.sample_size === 0 ? "all" : "sample"}
          onValueChange={(v) => update({ sample_size: v === "all" ? 0 : 1000 })}
          disabled={disabled}
        >
          <SelectTrigger className="w-48"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t("runs.allRows")}</SelectItem>
            <SelectItem value="sample">{t("runs.sampleRows")}</SelectItem>
          </SelectContent>
        </Select>
        {(config.sample_size ?? 1000) > 0 && (
          <div className="flex items-center gap-2">
            <Label className="text-xs text-muted-foreground whitespace-nowrap">{t("runs.sampleSizePerTable")}</Label>
            <Input
              type="number"
              min={1}
              max={10000}
              value={config.sample_size ?? 1000}
              onChange={(e) => update({ sample_size: Number(e.target.value) })}
              disabled={disabled}
              className="w-32"
            />
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Resolve schedule scope to list of table FQNs
// ---------------------------------------------------------------------------

function resolveScheduleScope(
  cfg: ScheduleConfig,
  approvedRules: RuleCatalogEntryOut[],
): string[] {
  const labelTokens = new Set<string>(
    (cfg.scope_labels ?? []).map(({ key, value }) => labelToken(key, value)),
  );
  return approvedRules
    .filter((r) => {
      const { catalog, schema } = parseFqn(r.table_fqn);
      const fqnMatches = (() => {
        switch (cfg.scope_mode) {
          case "all": return true;
          case "catalog": return (cfg.scope_catalogs ?? []).includes(catalog);
          case "schema": return (cfg.scope_schemas ?? []).includes(`${catalog}.${schema}`);
          case "tables": return (cfg.scope_tables ?? []).includes(r.table_fqn);
          default: return true;
        }
      })();
      if (!fqnMatches) return false;
      return ruleMatchesLabels(r, labelTokens);
    })
    .map((r) => r.table_fqn);
}

// ---------------------------------------------------------------------------
// Main page — two top-level tabs: Execute, Schedules
// ---------------------------------------------------------------------------

function RunsPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { canRunRules, isAdmin } = usePermissions();
  const params = useParams({ strict: false }) as { runName?: string };
  const currentRunName = params.runName;

  // The Run Rules page is gated on the orthogonal RUNNER role (admins
  // are implicit runners). Anyone who lands here without the privilege —
  // whether by typing the URL or via a stale link — gets bounced to
  // Runs History, which is universally readable.
  if (!canRunRules) {
    return <Navigate to="/runs-history" replace />;
  }
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [isDeletingRun, setIsDeletingRun] = useState(false);
  const queryClient = useQueryClient();

  const [scheduleNames, setScheduleNames] = useState<string[]>([]);

  useEffect(() => {
    fetchSchedules().then((entries) => {
      setScheduleNames(entries.map((e) => e.schedule_name));
    }).catch((err) => {
      if (isAxiosError(err) && err.response?.status === 403) {
        toast.error(t("runs.permissionsViewSchedules"));
      } else {
        toast.error(t("runs.failedLoadSchedules"));
      }
    });
  }, [t]);

  const handleCreateRun = async (name: string) => {
    try {
      await saveScheduleApi(name, { ...DEFAULT_SCHEDULE });
      await queryClient.refetchQueries({ queryKey: [...SCHEDULES_KEY] });
      setScheduleNames((prev) => [...prev, name]);
      toast.success(t("runs.scheduleCreated", { name }));
      navigate({ to: "/runs/$runName", params: { runName: name } });
      setIsCreateOpen(false);
    } catch (error: unknown) {
      if (isAxiosError(error) && error.response?.status === 403) {
        toast.error(t("runs.permissionsCreateSchedules"));
      } else {
        const detail =
          error instanceof Error ? error.message :
          typeof error === "object" && error !== null && "response" in error
            ? String((error as { response?: { data?: { detail?: string } } }).response?.data?.detail ?? "")
            : "";
        toast.error(detail || t("runs.failedCreateSchedule"));
      }
      console.error(error);
      throw error;
    }
  };

  const [activeTab, setActiveTab] = useState<string>(
    currentRunName ? "schedules" : "execute",
  );

  useEffect(() => {
    if (currentRunName) setActiveTab("schedules");
  }, [currentRunName]);

  return (
    <div className="flex flex-col h-full">
      <PageBreadcrumb
        items={currentRunName ? [{ label: t("runs.title"), to: "/runs" }] : []}
        page={currentRunName || t("runs.title")}
      />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col mt-4 overflow-hidden">
        <div className="flex items-center justify-between shrink-0 mb-4">
          <TabsList>
            <TabsTrigger value="execute" className="gap-2">
              <Zap className="h-4 w-4" />
              {t("runs.tabExecute")}
            </TabsTrigger>
            <TabsTrigger value="schedules" className="gap-2">
              <CalendarClock className="h-4 w-4" />
              {t("runs.tabSchedules")}
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="execute" className="flex-1 overflow-hidden mt-0">
          <QueryErrorResetBoundary>
            {({ reset }) => (
              <ErrorBoundary onReset={reset} fallbackRender={ExecuteTabError}>
                <Suspense fallback={<ExecuteTabSkeleton />}>
                  <ExecuteTab onGoToHistory={() => navigate({ to: "/runs-history" })} />
                </Suspense>
              </ErrorBoundary>
            )}
          </QueryErrorResetBoundary>
        </TabsContent>

        <TabsContent value="schedules" className="flex-1 overflow-hidden mt-0">
          {currentRunName ? (
            <div className="flex flex-col flex-1 overflow-hidden h-full">
              <div className="shrink-0 mb-4">
                <Button
                  variant="ghost"
                  size="sm"
                  className="gap-1.5 text-muted-foreground hover:text-foreground -ml-2"
                  onClick={() => navigate({ to: "/runs" })}
                >
                  <ChevronRight className="h-4 w-4 rotate-180" />
                  {t("runs.backToSchedules")}
                </Button>
              </div>
              <div className="flex-1 overflow-hidden flex flex-col">
                <QueryErrorResetBoundary>
                  {({ reset }) => (
                    <ErrorBoundary onReset={reset} fallbackRender={RunEditorError}>
                      <Suspense fallback={<RunEditorSkeleton />}>
                        <RunEditorContainer
                          currentRunName={currentRunName}
                          onAddRun={() => setIsCreateOpen(true)}
                          onDeletingChange={setIsDeletingRun}
                          isAdmin={isAdmin}
                        />
                      </Suspense>
                    </ErrorBoundary>
                  )}
                </QueryErrorResetBoundary>
              </div>
            </div>
          ) : (
            <div className="flex flex-col flex-1 overflow-hidden h-full">
              <div className="flex items-center justify-between mb-4 shrink-0">
                <h2 className="font-semibold text-lg text-foreground">{t("runs.scheduledRuns")}</h2>
                {isAdmin && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setIsCreateOpen(true)}
                  className="gap-2"
                >
                  <Plus className="h-4 w-4" />
                  {t("runs.newSchedule")}
                </Button>
                )}
              </div>
              <div className="flex-1 overflow-y-auto">
                <QueryErrorResetBoundary>
                  {({ reset }) => (
                    <ErrorBoundary onReset={reset} fallbackRender={RunsListError}>
                      <Suspense fallback={<RunsListSkeleton />}>
                        <SchedulesListView isDeleting={isDeletingRun} />
                      </Suspense>
                    </ErrorBoundary>
                  )}
                </QueryErrorResetBoundary>
              </div>
            </div>
          )}
        </TabsContent>

      </Tabs>

      <CreateRunDialog
        open={isCreateOpen}
        onOpenChange={setIsCreateOpen}
        existingNames={scheduleNames}
        onCreate={handleCreateRun}
      />
    </div>
  );
}

// ===========================================================================
// Execute Tab — manual trigger with grouping by catalog/schema/table
// ===========================================================================

interface RunNotification {
  count: number;
  errors: number;
}

function ExecuteTab({ onGoToHistory }: { onGoToHistory: () => void }) {
  const { t } = useTranslation();
  const { data: rulesResp, isLoading: rulesLoading } = useListRules(
    { status: "approved" },
    { query: {} },
  );
  const approvedRules: RuleCatalogEntryOut[] = useMemo(() => {
    const rawRules = Array.isArray(rulesResp?.data)
      ? rulesResp.data.filter((r) => r.status === "approved")
      : [];
    const byTable = new Map<string, RuleCatalogEntryOut>();
    for (const rule of rawRules) {
      const existing = byTable.get(rule.table_fqn);
      if (existing) {
        existing.checks = [...existing.checks, ...rule.checks];
      } else {
        byTable.set(rule.table_fqn, { ...rule, checks: [...rule.checks] });
      }
    }
    return Array.from(byTable.values());
  }, [rulesResp]);

  const [groupBy, setGroupBy] = useState<GroupMode>("catalog");
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  const [sampleSize, setSampleSize] = useState(1000);
  const [searchQuery, setSearchQuery] = useState("");
  const [filterCatalog, setFilterCatalog] = useState<string>("__all__");
  const [filterSchema, setFilterSchema] = useState<string>("__all__");
  const [labelFilter, setLabelFilter] = useState<Set<string>>(new Set());
  const [isRunning, setIsRunning] = useState(false);
  const [runNotification, setRunNotification] = useState<RunNotification | null>(null);

  const { activeRuns, addRuns, removeRun, clearAll: clearActiveRuns } = useActiveRuns();
  const runningFqns = useMemo(
    () => new Set(activeRuns.map((r) => r.table_fqn)),
    [activeRuns],
  );
  const batchRun = useBatchRunFromCatalog();
  const queryClient = useQueryClient();

  const crossTableRulesLabel = t("runs.crossTableRules");

  const allCatalogs = useMemo(() => {
    const cats = new Set<string>();
    for (const r of approvedRules) {
      if (r.table_fqn.startsWith(_SQL_CHECK_PREFIX)) {
        cats.add(crossTableRulesLabel);
      } else {
        cats.add(parseFqn(r.table_fqn).catalog);
      }
    }
    return Array.from(cats).sort();
  }, [approvedRules, crossTableRulesLabel]);

  const availableSchemas = useMemo(() => {
    const schemas = new Set<string>();
    for (const r of approvedRules) {
      if (r.table_fqn.startsWith(_SQL_CHECK_PREFIX)) continue;
      const { catalog, schema } = parseFqn(r.table_fqn);
      if (filterCatalog !== "__all__" && catalog !== filterCatalog) continue;
      schemas.add(`${catalog}.${schema}`);
    }
    return Array.from(schemas).sort();
  }, [approvedRules, filterCatalog]);

  useEffect(() => {
    if (filterSchema !== "__all__" && !availableSchemas.includes(filterSchema)) {
      setFilterSchema("__all__");
    }
  }, [availableSchemas, filterSchema]);

  const availableLabels = useMemo(
    () => collectAvailableLabels(approvedRules),
    [approvedRules],
  );

  const filteredRules = useMemo(() => {
    return approvedRules.filter((r) => {
      const isSqlCheck = r.table_fqn.startsWith(_SQL_CHECK_PREFIX);
      if (filterCatalog !== "__all__") {
        if (filterCatalog === crossTableRulesLabel) {
          if (!isSqlCheck) return false;
        } else {
          if (isSqlCheck) return false;
          const { catalog, schema } = parseFqn(r.table_fqn);
          if (catalog !== filterCatalog) return false;
          if (filterSchema !== "__all__" && `${catalog}.${schema}` !== filterSchema) return false;
        }
      } else if (!isSqlCheck && filterSchema !== "__all__") {
        const { catalog, schema } = parseFqn(r.table_fqn);
        if (`${catalog}.${schema}` !== filterSchema) return false;
      }
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        if (!(r.display_name || r.table_fqn).toLowerCase().includes(q)) return false;
      }
      if (!ruleMatchesLabels(r, labelFilter)) return false;
      return true;
    });
  }, [approvedRules, filterCatalog, filterSchema, searchQuery, crossTableRulesLabel, labelFilter]);

  const grouped = useMemo(() => {
    const groups = new Map<string, RuleCatalogEntryOut[]>();

    for (const rule of filteredRules) {
      const isSqlCheck = rule.table_fqn.startsWith(_SQL_CHECK_PREFIX);
      let key: string;
      if (isSqlCheck) {
        key = crossTableRulesLabel;
      } else {
        const { catalog, schema } = parseFqn(rule.table_fqn);
        switch (groupBy) {
          case "catalog":
            key = catalog || t("runs.unknown");
            break;
          case "schema":
            key = `${catalog}.${schema}` || t("runs.unknown");
            break;
          default:
            key = t("runs.all");
        }
      }
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(rule);
    }

    return new Map([...groups.entries()].sort(([a], [b]) => a.localeCompare(b)));
  }, [filteredRules, groupBy, crossTableRulesLabel, t]);

  const toggleTable = useCallback((tableFqn: string) => {
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(tableFqn)) next.delete(tableFqn);
      else next.add(tableFqn);
      return next;
    });
  }, []);

  const toggleGroup = useCallback((tables: RuleCatalogEntryOut[]) => {
    setSelectedTables((prev) => {
      const next = new Set(prev);
      const allSelected = tables.every((r) => next.has(r.table_fqn));
      if (allSelected) {
        tables.forEach((r) => next.delete(r.table_fqn));
      } else {
        tables.forEach((r) => next.add(r.table_fqn));
      }
      return next;
    });
  }, []);

  const selectAll = useCallback(() => {
    setSelectedTables(new Set(filteredRules.map((r) => r.table_fqn)));
  }, [filteredRules]);

  const clearAll = useCallback(() => {
    setSelectedTables(new Set());
  }, []);

  const handleRunSelected = async () => {
    if (selectedTables.size === 0) return;
    setIsRunning(true);
    const tableFqns = Array.from(selectedTables);
    try {
      const resp = await batchRun.mutateAsync({
        data: {
          table_fqns: tableFqns,
          sample_size: sampleSize,
        },
      });
      const result = resp.data;
      if (result.submitted.length > 0) {
        const now = Date.now();
        addRuns(
          result.submitted.map((s, i) => ({
            run_id: s.run_id,
            job_run_id: s.job_run_id,
            view_fqn: s.view_fqn,
            table_fqn: tableFqns[i] ?? "unknown",
            submitted_at: now,
          })),
        );
        queryClient.invalidateQueries({ queryKey: getListValidationRunsQueryKey() });
      }
      setRunNotification({
        count: result.submitted.length,
        errors: result.errors.length,
      });
      setSelectedTables(new Set());
    } catch (err) {
      toast.error(t("runs.batchRunFailed", { error: err instanceof Error ? err.message : t("common.unknownError") }));
    } finally {
      setIsRunning(false);
    }
  };

  if (rulesLoading) return <ExecuteTabSkeleton />;

  return (
    <div className="space-y-6 h-full overflow-y-auto">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold tracking-tight">{t("runs.executeRules")}</h2>
          <p className="text-muted-foreground text-sm">
            {t("runs.executeRulesSubtitle")}
          </p>
        </div>
      </div>

      {runNotification && (
        <div className="flex items-center justify-between rounded-lg border px-4 py-3 bg-muted/40">
          <div className="flex items-center gap-2 text-sm">
            {runNotification.count > 0 ? (
              <>
                <CheckCircle2 className="h-4 w-4 text-green-600 shrink-0" />
                <span>
                  {t("runs.startedRuns", { count: runNotification.count })}
                  {runNotification.errors > 0 && (
                    <span className="text-destructive ml-1">
                      {t("runs.runsFailedToSubmit", { count: runNotification.errors })}
                    </span>
                  )}
                </span>
              </>
            ) : (
              <>
                <XCircle className="h-4 w-4 text-destructive shrink-0" />
                <span className="text-destructive">
                  {t("runs.allTablesFailedToSubmit", { count: runNotification.errors })}
                </span>
              </>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            {runNotification.count > 0 && (
              <Button
                variant="outline"
                size="sm"
                className="gap-1.5 text-xs"
                onClick={onGoToHistory}
              >
                <History className="h-3.5 w-3.5" />
                {t("runs.viewInHistory")}
              </Button>
            )}
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0 text-muted-foreground"
              onClick={() => setRunNotification(null)}
            >
              <X className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      )}

      {approvedRules.length === 0 ? (
        <Card>
          <CardContent className="py-16">
            <div className="flex flex-col items-center justify-center text-center">
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
                <Play className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-muted-foreground">
                {t("runs.noApprovedRules")}
              </h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                {t("runs.noApprovedRulesDescription")}
              </p>
              <Button variant="outline" className="mt-4" asChild>
                <Link to="/rules/drafts">{t("runs.goToDraftsReview")}</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* Controls bar */}
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Zap className="h-4 w-4" />
                    {t("runs.tableSelection")}
                  </CardTitle>
                  <CardDescription>
                    {t("runs.approvedRuleSetsAvailable", { count: approvedRules.length })}
                    {selectedTables.size > 0 && (
                      <span className="text-primary font-medium ml-1">
                        {t("runs.selectedSuffix", { count: selectedTables.size })}
                      </span>
                    )}
                  </CardDescription>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex items-center gap-2">
                    <Select
                      value={sampleSize === 0 ? "all" : "sample"}
                      onValueChange={(v) => setSampleSize(v === "all" ? 0 : 1000)}
                    >
                      <SelectTrigger className="w-32 h-8 text-xs"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">{t("runs.allRows")}</SelectItem>
                        <SelectItem value="sample">{t("runs.sampleRows")}</SelectItem>
                      </SelectContent>
                    </Select>
                    {sampleSize > 0 && (
                      <Input
                        id="sample-size-exec"
                        type="number"
                        min={1}
                        max={10000}
                        value={sampleSize}
                        onChange={(e) => setSampleSize(Number(e.target.value))}
                        className="w-24 h-8 text-xs"
                      />
                    )}
                  </div>
                  <Button
                    onClick={handleRunSelected}
                    disabled={selectedTables.size === 0 || isRunning}
                    className="gap-2"
                  >
                    {isRunning ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                    {isRunning
                      ? t("runs.running")
                      : selectedTables.size > 0
                        ? t("runs.runNSelected", { count: selectedTables.size })
                        : t("runs.runSelected")}
                  </Button>
                </div>
              </div>

              <div className="flex items-center gap-2 flex-wrap pt-3 border-t mt-3">
                <div className="flex items-center gap-1.5">
                  <Layers className="h-3.5 w-3.5 text-muted-foreground" />
                  <span className="text-xs text-muted-foreground">{t("runs.groupBy")}</span>
                  <Select value={groupBy} onValueChange={(v) => setGroupBy(v as GroupMode)}>
                    <SelectTrigger className="w-[120px] h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="catalog" className="text-xs">
                        <span className="flex items-center gap-1.5"><Database className="h-3 w-3" /> {t("runs.groupByCatalog")}</span>
                      </SelectItem>
                      <SelectItem value="schema" className="text-xs">
                        <span className="flex items-center gap-1.5"><Layers className="h-3 w-3" /> {t("runs.groupBySchema")}</span>
                      </SelectItem>
                      <SelectItem value="none" className="text-xs">
                        <span className="flex items-center gap-1.5"><Table2 className="h-3 w-3" /> {t("runs.groupByFlat")}</span>
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center gap-1.5">
                  <Database className="h-3.5 w-3.5 text-muted-foreground" />
                  <Select value={filterCatalog} onValueChange={(v) => { setFilterCatalog(v); setFilterSchema("__all__"); }}>
                    <SelectTrigger className="w-[140px] h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__all__" className="text-xs">{t("runs.allCatalogs")}</SelectItem>
                      {allCatalogs.map((c) => (
                        <SelectItem key={c} value={c} className="text-xs">{c}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center gap-1.5">
                  <Layers className="h-3.5 w-3.5 text-muted-foreground" />
                  <Select value={filterSchema} onValueChange={(v) => setFilterSchema(v)}>
                    <SelectTrigger className="w-[200px] h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__all__" className="text-xs">{t("runs.allSchemas")}</SelectItem>
                      {availableSchemas.map((s) => (
                        <SelectItem key={s} value={s} className="text-xs">{s}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <LabelFilter
                  available={availableLabels}
                  selected={labelFilter}
                  onChange={setLabelFilter}
                  className="h-8"
                />

                <div className="relative flex-1 max-w-xs">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder={t("runs.searchTablesPlaceholder")}
                    className="h-8 pl-8 text-xs"
                  />
                </div>

                <div className="flex items-center gap-1.5 ml-auto">
                  <Button variant="ghost" size="sm" className="h-8 text-xs" onClick={selectAll}>
                    {t("runs.selectAll")}
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 text-xs"
                    onClick={clearAll}
                    disabled={selectedTables.size === 0}
                  >
                    {t("runs.clear")}
                  </Button>
                </div>
              </div>
            </CardHeader>

            <CardContent>
              {filteredRules.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground text-sm">
                  {t("runs.noRulesMatchSearch")}
                </div>
              ) : groupBy === "none" ? (
                <FadeIn duration={0.3}>
                  <RuleTable
                    rules={filteredRules}
                    selectedTables={selectedTables}
                    onToggle={toggleTable}
                    runningFqns={runningFqns}
                  />
                </FadeIn>
              ) : (
                <FadeIn duration={0.3}>
                  <div className="space-y-4">
                    {Array.from(grouped.entries()).map(([group, rules]) => {
                      const allSelected = rules.every((r) => selectedTables.has(r.table_fqn));
                      const someSelected = rules.some((r) => selectedTables.has(r.table_fqn));
                      return (
                        <div key={group} className="border rounded-lg overflow-hidden">
                          <div className="flex items-center gap-3 p-3 bg-muted/40 border-b">
                            <Checkbox
                              checked={allSelected ? true : someSelected ? "indeterminate" : false}
                              onCheckedChange={() => toggleGroup(rules)}
                            />
                            <div className="flex items-center gap-2">
                              {groupBy === "catalog" && <Database className="h-3.5 w-3.5 text-muted-foreground" />}
                              {groupBy === "schema" && <Layers className="h-3.5 w-3.5 text-muted-foreground" />}
                              <span className="text-sm font-medium">{group}</span>
                            </div>
                            <Badge variant="secondary" className="ml-auto text-xs">
                              {t("runs.tablesCount", { count: rules.length })}
                            </Badge>
                          </div>
                          <RuleTable
                            rules={rules}
                            selectedTables={selectedTables}
                            onToggle={toggleTable}
                            runningFqns={runningFqns}
                          />
                        </div>
                      );
                    })}
                  </div>
                </FadeIn>
              )}
            </CardContent>
          </Card>

          {/* Active runs banner */}
          {activeRuns.length > 0 && (
            <ActiveRunsCard
              runs={activeRuns}
              onRunComplete={(runId) => {
                removeRun(runId);
                queryClient.invalidateQueries({ queryKey: getListValidationRunsQueryKey() });
                notifyRuns({ run_ids: [runId], trigger: "manual" }).catch(() => {});
              }}
              onDismissAll={() => {
                clearActiveRuns();
                queryClient.invalidateQueries({ queryKey: getListValidationRunsQueryKey() });
              }}
            />
          )}
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Active Runs Card — shows in-progress runs with polling
// ---------------------------------------------------------------------------

const TERMINAL_STATES = new Set(["TERMINATED", "INTERNAL_ERROR", "SKIPPED"]);

function ActiveRunsCard({
  runs,
  onRunComplete,
  onDismissAll,
}: {
  runs: ActiveRun[];
  onRunComplete: (runId: string) => void;
  onDismissAll: () => void;
}) {
  const { t } = useTranslation();
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2 text-base">
              <Loader2 className="h-4 w-4 animate-spin text-amber-500" />
              {t("runs.activeRuns")}
              <Badge variant="secondary" className="text-xs">{runs.length}</Badge>
            </CardTitle>
            <CardDescription>
              {t("runs.activeRunsDescription")}
            </CardDescription>
          </div>
          <Button variant="ghost" size="sm" className="text-xs text-muted-foreground" onClick={onDismissAll}>
            {t("runs.dismissAll")}
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                <th className="text-left p-3 font-medium">{t("runs.headerTable")}</th>
                <th className="text-left p-3 font-medium">{t("runs.headerRunId")}</th>
                <th className="text-left p-3 font-medium">{t("runs.headerStatus")}</th>
                <th className="text-left p-3 font-medium">{t("runs.headerElapsed")}</th>
                <th className="w-10 p-3" />
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <ActiveRunRow key={run.run_id} run={run} onComplete={onRunComplete} />
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

function ActiveRunRow({
  run,
  onComplete,
}: {
  run: ActiveRun;
  onComplete: (runId: string) => void;
}) {
  const { t } = useTranslation();
  const [status, setStatus] = useState<RunStatusOut | null>(null);
  const [pollError, setPollError] = useState(false);
  const [isCancelling, setIsCancelling] = useState(false);
  const onCompleteRef = useRef(onComplete);
  onCompleteRef.current = onComplete;

  useEffect(() => {
    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout>;

    const poll = async () => {
      try {
        const resp = await getDryRunStatus(run.run_id, {
          job_run_id: run.job_run_id,
          view_fqn: run.view_fqn,
        });
        if (cancelled) return;
        setStatus(resp.data);
        setPollError(false);

        if (TERMINAL_STATES.has(resp.data.state)) {
          const tableName = cleanFqn(run.table_fqn).split(".").pop() ?? "";
          if (resp.data.state === "TERMINATED" && resp.data.result_state === "SUCCESS") {
            toast.success(t("runs.runCompleted", { table: tableName }));
          } else if (resp.data.state === "INTERNAL_ERROR") {
            toast.error(t("runs.runFailedInternal", { table: tableName }));
          } else {
            toast.info(t("runs.runFinished", { table: tableName, state: resp.data.result_state ?? resp.data.state }));
          }
          onCompleteRef.current(run.run_id);
          return;
        }
      } catch {
        if (cancelled) return;
        setPollError(true);
      }
      timeoutId = setTimeout(poll, 5000);
    };

    poll();
    return () => {
      cancelled = true;
      clearTimeout(timeoutId);
    };
  }, [run.run_id, run.job_run_id, run.view_fqn, run.table_fqn, t]);

  const stateLabel = status?.state ?? "PENDING";
  const isStillRunning = !TERMINAL_STATES.has(stateLabel);
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(interval);
  }, []);
  const elapsed = Math.round((now - run.submitted_at) / 1000);
  const elapsedStr =
    elapsed < 60 ? `${elapsed}s` : `${Math.floor(elapsed / 60)}m ${elapsed % 60}s`;

  const handleStop = async (e: React.MouseEvent) => {
    e.stopPropagation();
    setIsCancelling(true);
    try {
      await cancelDryRun(run.run_id, { job_run_id: run.job_run_id });
      const tableName = cleanFqn(run.table_fqn).split(".").pop() ?? "";
      toast.info(t("runs.runCanceled", { table: tableName }));
      onCompleteRef.current(run.run_id);
    } catch {
      toast.error(t("runs.failedCancelRun"));
    } finally {
      setIsCancelling(false);
    }
  };

  return (
    <tr className="border-b last:border-b-0">
      <td className="p-3 font-mono text-xs">{cleanFqn(run.table_fqn)}</td>
      <td className="p-3 font-mono text-xs text-muted-foreground">{run.run_id}</td>
      <td className="p-3">
        {pollError ? (
          <Badge variant="secondary" className="gap-1 text-xs">
            <AlertCircle className="h-3 w-3" />
            {t("runs.statusPollingError")}
          </Badge>
        ) : stateLabel === "INTERNAL_ERROR" ? (
          <Badge variant="outline" className="gap-1 border-red-500 text-red-600 text-xs">
            <XCircle className="h-3 w-3" />
            {t("runs.statusInternalError")}
          </Badge>
        ) : stateLabel === "RUNNING" ? (
          <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600 text-xs">
            <Loader2 className="h-3 w-3 animate-spin" />
            {t("runs.statusRunning")}
          </Badge>
        ) : stateLabel === "PENDING" ? (
          <Badge variant="outline" className="gap-1 border-blue-500 text-blue-600 text-xs">
            <Clock className="h-3 w-3" />
            {t("runs.statusPending")}
          </Badge>
        ) : stateLabel === "TERMINATED" ? (
          <Badge variant="outline" className="gap-1 border-green-500 text-green-600 text-xs">
            <CheckCircle2 className="h-3 w-3" />
            {status?.result_state ?? t("runs.statusDone")}
          </Badge>
        ) : (
          <Badge variant="secondary" className="text-xs">{stateLabel}</Badge>
        )}
      </td>
      <td className="p-3 text-xs text-muted-foreground">{elapsedStr}</td>
      <td className="p-3">
        <div className="flex items-center gap-1">
          {isStillRunning && (
            <Button
              variant="ghost"
              size="sm"
              className="h-6 px-1.5 gap-1 text-red-600 hover:text-red-700 hover:bg-red-50"
              onClick={handleStop}
              disabled={isCancelling}
              title={t("runs.stopRun")}
            >
              {isCancelling ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : (
                <CircleStop className="h-3.5 w-3.5" />
              )}
              <span className="text-xs">{t("runs.stop")}</span>
            </Button>
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0"
            onClick={() => onComplete(run.run_id)}
            title={t("runs.dismiss")}
          >
            <XCircle className="h-3.5 w-3.5 text-muted-foreground" />
          </Button>
        </div>
      </td>
    </tr>
  );
}

function RuleTable({
  rules,
  selectedTables,
  onToggle,
  runningFqns,
}: {
  rules: RuleCatalogEntryOut[];
  selectedTables: Set<string>;
  onToggle: (tableFqn: string) => void;
  runningFqns?: Set<string>;
}) {
  const { t } = useTranslation();
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b bg-muted/30">
          <th className="w-10 p-3" />
          <th className="text-left p-3 font-medium">{t("runs.headerTable")}</th>
          <th className="text-right p-3 font-medium">{t("runs.headerRules")}</th>
          <th className="text-left p-3 font-medium">{t("runs.headerStatus")}</th>
        </tr>
      </thead>
      <tbody>
        {rules.map((rule) => {
          const isTableRunning = runningFqns?.has(rule.table_fqn);
          return (
            <tr
              key={rule.table_fqn}
              className={cn(
                "border-b last:border-b-0 hover:bg-muted/20 transition-colors cursor-pointer",
                selectedTables.has(rule.table_fqn) && "bg-primary/5",
              )}
              onClick={() => onToggle(rule.table_fqn)}
            >
              <td className="p-3 text-center">
                <Checkbox
                  checked={selectedTables.has(rule.table_fqn)}
                  onCheckedChange={() => onToggle(rule.table_fqn)}
                  onClick={(e: React.MouseEvent) => e.stopPropagation()}
                />
              </td>
              <td className="p-3 font-mono text-xs">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="truncate">{rule.display_name || rule.table_fqn}</span>
                  <LabelsBadges labels={collectRuleLabels(rule)} max={3} size="xs" />
                </div>
              </td>
              <td className="p-3 text-right tabular-nums">{rule.checks.length}</td>
              <td className="p-3">
                <div className="flex items-center gap-1.5">
                  <Badge variant="outline" className="gap-1 border-green-500 text-green-600 text-xs">
                    <CheckCircle2 className="h-3 w-3" />
                    {t("runs.approved")}
                  </Badge>
                  {isTableRunning && (
                    <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600 text-xs">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      {t("runs.statusRunning")}
                    </Badge>
                  )}
                </div>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function ExecuteTabSkeleton() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-7 w-40" />
          <Skeleton className="h-4 w-72" />
        </div>
      </div>
      <Skeleton className="h-96 w-full" />
    </div>
  );
}

function ExecuteTabError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("runs.failedLoadRules")}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2 mt-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

// ===========================================================================
// Schedules Tab — preserved configuration editor
// ===========================================================================

function CreateRunDialog({
  open,
  onOpenChange,
  existingNames,
  onCreate,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  existingNames: string[];
  onCreate: (name: string) => Promise<void>;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (open) {
      setName("");
      setIsSubmitting(false);
    }
  }, [open]);

  const isConflict = existingNames.includes(name);
  const isFormatValid = /^[a-zA-Z0-9_-]{1,64}$/.test(name);
  const isValid = name.trim().length > 0 && !isConflict && isFormatValid;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isValid) return;
    setIsSubmitting(true);
    try {
      await onCreate(name);
    } catch {
      setIsSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>{t("runs.createSchedule")}</DialogTitle>
          <DialogDescription>
            {t("runs.createScheduleDescription")}
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit}>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="name">{t("runs.name")}</Label>
              <Input
                id="name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className={cn((isConflict || (name.length > 0 && !isFormatValid)) && "border-destructive focus-visible:ring-destructive")}
                placeholder={t("runs.namePlaceholder")}
                autoFocus
                autoComplete="off"
              />
              {isConflict && (
                <p className="text-xs text-destructive">{t("runs.scheduleNameExists")}</p>
              )}
              {!isConflict && name.length > 0 && !isFormatValid && (
                <p className="text-xs text-destructive">
                  {t("runs.scheduleNameInvalid")}
                </p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>{t("runs.cancel")}</Button>
            <Button type="submit" disabled={!isValid || isSubmitting}>
              {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
              {t("runs.create")}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function RunsListSkeleton() {
  return (
    <div className="space-y-2">
      {[1, 2, 3, 4].map((i) => <Skeleton key={i} className="h-10 w-full" />)}
    </div>
  );
}

function RunEditorSkeleton() {
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between pb-4 border-b border-border/50">
        <div className="space-y-2">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-64" />
        </div>
        <div className="flex gap-2">
          <Skeleton className="h-9 w-20" />
          <Skeleton className="h-9 w-20" />
          <Skeleton className="h-9 w-9" />
        </div>
      </div>
      <div className="flex-1 mt-4">
        <Skeleton className="h-full w-full rounded-lg" />
      </div>
    </div>
  );
}

function RunsListError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-8 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("runs.failedLoadSchedules")}</p>
      <p className="text-muted-foreground/70 text-xs mb-3">
        {t("runs.failedLoadSchedulesHint")}
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

function RunEditorError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center h-full text-center">
      <AlertCircle className="h-16 w-16 text-destructive/30 mb-4" />
      <h3 className="text-lg font-semibold mb-2">{t("runs.failedLoadEditor")}</h3>
      <p className="text-muted-foreground text-sm mb-4">
        {t("runs.failedLoadEditorHint")}
      </p>
      <Button variant="outline" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-4 w-4" />
        {t("common.retry")}
      </Button>
    </div>
  );
}

// ===========================================================================
// Schedules List View — full-width table with metadata columns
// ===========================================================================

function scopeLabel(cfg: ScheduleConfig, t: Translator): string {
  switch (cfg.scope_mode) {
    case "all": return t("runs.scopeAll");
    case "catalog": return (cfg.scope_catalogs ?? []).join(", ") || t("runs.scopeNoCatalogs");
    case "schema": return (cfg.scope_schemas ?? []).join(", ") || t("runs.scopeNoSchemas");
    case "tables": {
      const tables = cfg.scope_tables ?? [];
      if (tables.length <= 2) return tables.join(", ") || t("runs.scopeNoTables");
      return t("runs.scopeMoreSummary", { a: tables[0], b: tables[1], count: tables.length - 2 });
    }
    default: return t("runs.scopeAllLabel");
  }
}

function SchedulesListView({ isDeleting }: { isDeleting?: boolean }) {
  const { t } = useTranslation();
  const [schedules, setSchedules] = useState<ScheduleConfigEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState<"permission" | "other" | null>(null);
  const prevDeleting = useRef(isDeleting);

  const { data: rulesResp } = useListRules({ status: "approved" }, { query: {} });
  const approvedRules: RuleCatalogEntryOut[] = useMemo(
    () => (Array.isArray(rulesResp?.data) ? rulesResp.data.filter((r: RuleCatalogEntryOut) => r.status === "approved") : []),
    [rulesResp],
  );

  const handleCopy = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      toast.success(t("runs.endpointCopied"), { duration: 1500 });
    } catch {
      toast.error(t("runs.couldNotCopy"));
    }
  };

  const handleFetchError = (err: unknown) => {
    setFetchError(isAxiosError(err) && err.response?.status === 403 ? "permission" : "other");
  };

  useEffect(() => {
    const wasDeleting = prevDeleting.current;
    prevDeleting.current = isDeleting;
    if (wasDeleting && !isDeleting) {
      fetchSchedules().then(setSchedules).catch(handleFetchError);
      return;
    }
    if (!wasDeleting || loading) {
      setFetchError(null);
      fetchSchedules()
        .then(setSchedules)
        .catch(handleFetchError)
        .finally(() => setLoading(false));
    }
  }, [isDeleting]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (fetchError === "permission") {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <ShieldAlert className="h-12 w-12 text-destructive/30 mb-3" />
        <p className="text-destructive text-sm mb-1">{t("runs.permissionsViewSchedulesTitle")}</p>
        <p className="text-muted-foreground/70 text-xs">
          {t("runs.permissionsViewSchedulesBody")}
        </p>
      </div>
    );
  }

  if (fetchError) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
        <p className="text-destructive text-sm mb-1">{t("runs.failedLoadSchedules")}</p>
        <p className="text-muted-foreground/70 text-xs">
          {t("runs.tryRefreshing")}
        </p>
      </div>
    );
  }

  if (schedules.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <CalendarClock className="h-12 w-12 text-muted-foreground/30 mb-3" />
        <p className="text-muted-foreground text-sm mb-1">{t("runs.noSchedulesConfigured")}</p>
        <p className="text-muted-foreground/70 text-xs">
          {t("runs.clickNewScheduleHint")}
        </p>
      </div>
    );
  }

  return (
    <div className="border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b bg-muted/50">
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerName")}</th>
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerFrequency")}</th>
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerScope")}</th>
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerSampleSize")}</th>
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerLastUpdated")}</th>
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerUpdatedBy")}</th>
            <th className="text-left font-medium text-muted-foreground px-4 py-3">{t("runs.headerApi")}</th>
          </tr>
        </thead>
        <tbody>
          {schedules.map((sched) => {
            // Scope column shows the raw configured tables for "tables" mode; match
            // that instead of resolveScheduleScope's approved-rules filter so the
            // API popover never says "no tables" for a table the UI already lists.
            const scheduleTableFqns = sched.config.scope_mode === "tables"
              ? Array.from(new Set(sched.config.scope_tables ?? []))
              : Array.from(new Set(resolveScheduleScope(sched.config, approvedRules)));
            return (
            <tr key={sched.schedule_name} className="border-b last:border-b-0 hover:bg-muted/30 transition-colors">
              <td className="px-4 py-3">
                <Link
                  to="/runs/$runName"
                  params={{ runName: sched.schedule_name }}
                  className="font-medium text-primary hover:underline flex items-center gap-2"
                >
                  <CalendarClock className="h-4 w-4 shrink-0" />
                  {sched.schedule_name}
                </Link>
              </td>
              <td className="px-4 py-3 text-muted-foreground">
                <Badge variant="outline" className="font-normal capitalize">
                  {sched.config.frequency === "manual" ? t("runs.manual") : cronPreview(sched.config, t)}
                </Badge>
              </td>
              <td className="px-4 py-3 text-muted-foreground">
                <span className="truncate block max-w-[240px]" title={scopeLabel(sched.config, t)}>
                  {scopeLabel(sched.config, t)}
                </span>
              </td>
              <td className="px-4 py-3 text-muted-foreground">
                {sched.config.sample_size != null ? sched.config.sample_size.toLocaleString() : t("runs.allRows")}
              </td>
              <td className="px-4 py-3 text-muted-foreground text-xs">
                {sched.updated_at ? formatDate(sched.updated_at) : sched.created_at ? formatDate(sched.created_at) : "—"}
              </td>
              <td className="px-4 py-3 text-muted-foreground text-xs">
                {sched.updated_by || sched.created_by || "—"}
              </td>
              <td className="px-4 py-3">
                <Popover>
                  <PopoverTrigger asChild>
                    <Button variant="ghost" size="icon" aria-label={t("runs.viewApiEndpoints")}>
                      <Link2 className="h-4 w-4" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-[480px] max-h-96 overflow-y-auto space-y-2" align="end">
                    <div>
                      <p className="text-sm font-semibold">{t("runs.apiEndpointsTitle")}</p>
                      <p className="text-xs text-muted-foreground mt-0.5">{t("runs.apiEndpointsDescription")}</p>
                    </div>
                    {scheduleTableFqns.length === 0 ? (
                      <p className="text-xs text-muted-foreground">{t("runs.apiEndpointsNoTables")}</p>
                    ) : (
                      scheduleTableFqns.map((fqn) => {
                        const path = `/api/v1/alerts/status/table/${encodeURIComponent(fqn)}`;
                        const url = `${window.location.origin}${path}`;
                        return (
                          <div key={fqn} className="flex items-center gap-2" title={url}>
                            <Badge variant="secondary" className="shrink-0 font-mono text-[10px]">
                              GET
                            </Badge>
                            <code className="min-w-0 flex-1 truncate rounded bg-muted px-2 py-1 text-xs font-mono">
                              {path}
                            </code>
                            <Button variant="ghost" size="icon" onClick={() => handleCopy(url)} aria-label={t("runs.headerApi")}>
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                        );
                      })
                    )}
                    {(() => {
                      const runPath = "/api/v1/alerts/status/run/{run_id}";
                      const runUrl = `${window.location.origin}${runPath}`;
                      return (
                        <div className="flex items-center gap-2" title={t("runs.apiEndpointRunLabel")}>
                          <Badge variant="secondary" className="shrink-0 font-mono text-[10px]">
                            GET
                          </Badge>
                          <code className="min-w-0 flex-1 truncate rounded bg-muted px-2 py-1 text-xs font-mono">
                            {runPath}
                          </code>
                          <Button variant="ghost" size="icon" onClick={() => handleCopy(runUrl)} aria-label={t("runs.headerApi")}>
                            <Copy className="h-3 w-3" />
                          </Button>
                        </div>
                      );
                    })()}
                  </PopoverContent>
                </Popover>
              </td>
            </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function RunEditorContainer({
  currentRunName,
  onAddRun,
  onDeletingChange,
  isAdmin,
}: {
  currentRunName?: string;
  onAddRun: () => void;
  onDeletingChange: (isDeleting: boolean) => void;
  isAdmin: boolean;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { setRunContext } = useAIAssistant();

  const [isDeleteOpen, setIsDeleteOpen] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [scheduleEntry, setScheduleEntry] = useState<ScheduleConfigEntry | null>(null);
  const [loadingEntry, setLoadingEntry] = useState(false);
  const [entryNotFound, setEntryNotFound] = useState(false);

  useEffect(() => {
    onDeletingChange(isDeleting);
  }, [isDeleting, onDeletingChange]);

  useEffect(() => {
    if (!currentRunName) {
      setScheduleEntry(null);
      setEntryNotFound(false);
      return;
    }
    setLoadingEntry(true);
    fetchSchedule(currentRunName)
      .then((entry) => {
        setScheduleEntry(entry);
        setEntryNotFound(false);
      })
      .catch(() => {
        setScheduleEntry(null);
        setEntryNotFound(true);
      })
      .finally(() => setLoadingEntry(false));
  }, [currentRunName]);

  const [yamlContent, setYamlContent] = useState("");
  const [isDirty, setIsDirty] = useState(false);

  useEffect(() => {
    if (scheduleEntry) {
      try {
        const displayObj = {
          name: scheduleEntry.schedule_name,
          ...scheduleEntry.config,
        };
        const dump = yaml.dump(displayObj);
        setYamlContent(dump);
        setIsDirty(false);
      } catch (e) {
        console.error("Error converting config to YAML", e);
        toast.error(t("runs.errorParsingSchedule"));
      }
    } else {
      setYamlContent("");
      setIsDirty(false);
    }
  }, [scheduleEntry, t]);

  useEffect(() => {
    if (currentRunName && yamlContent) {
      setRunContext({ runName: currentRunName, yaml: yamlContent });
    } else {
      setRunContext(null);
    }
  }, [currentRunName, yamlContent, setRunContext]);

  useEffect(() => () => setRunContext(null), [setRunContext]);

  const handleSave = async () => {
    if (!currentRunName || !scheduleEntry) return;
    setIsSaving(true);
    try {
      const parsedYaml = yaml.load(yamlContent) as Record<string, any>;
      if (typeof parsedYaml !== "object" || !parsedYaml) throw new Error("Invalid YAML content");
      const { name: _name, ...configPart } = parsedYaml;
      const schedName = _name || currentRunName;
      const saved = await saveScheduleApi(schedName, configPart as ScheduleConfig);
      setScheduleEntry(saved);
      toast.success(t("runs.scheduleSaved"));
      setIsDirty(false);
      if (schedName !== currentRunName) {
        navigate({ to: "/runs/$runName", params: { runName: schedName } });
      }
      await queryClient.refetchQueries({ queryKey: [...SCHEDULES_KEY] });
    } catch (error) {
      console.error("Save error", error);
      if (isAxiosError(error) && error.response?.status === 403) {
        toast.error(t("runs.permissionsSaveSchedules"));
      } else {
        const message = error instanceof Error ? error.message : t("runs.yamlSyntaxOrValidation");
        toast.error(t("runs.failedSave", { message }));
      }
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!currentRunName) return;
    setIsDeleteOpen(false);
    setIsDeleting(true);

    try {
      await deleteScheduleApi(currentRunName);
      toast.success(t("runs.scheduleDeleted", { name: currentRunName }));
      navigate({ to: "/runs" });
      queryClient.refetchQueries({ queryKey: [...SCHEDULES_KEY] });
    } catch (error) {
      if (isAxiosError(error) && error.response?.status === 403) {
        toast.error(t("runs.permissionsDeleteSchedules"));
      } else {
        toast.error(t("runs.failedDeleteSchedule"));
      }
    } finally {
      setIsDeleting(false);
    }
  };

  const handleReset = () => {
    if (scheduleEntry) {
      try {
        const displayObj = {
          name: scheduleEntry.schedule_name,
          ...scheduleEntry.config,
        };
        const dump = yaml.dump(displayObj);
        setYamlContent(dump);
        setIsDirty(false);
        toast.info(t("runs.changesDiscarded"));
      } catch (e) {
        console.error("Error converting config to YAML", e);
      }
    }
  };

  const batchRun = useBatchRunFromCatalog();
  const [isRunningNow, setIsRunningNow] = useState(false);

  const handleRunNow = async () => {
    if (!scheduleEntry) return;
    setIsRunningNow(true);
    try {
      const cfg: ScheduleConfig = { ...DEFAULT_SCHEDULE, ...scheduleEntry.config };

      const resp = await fetch("/api/v1/rules?status=approved");
      const json = await resp.json();
      const allRules: RuleCatalogEntryOut[] = Array.isArray(json) ? json : [];

      const resolvedFqns = resolveScheduleScope(cfg, allRules);
      if (resolvedFqns.length === 0) {
        toast.error(t("runs.noScopeMatched"));
        return;
      }
      const result = await batchRun.mutateAsync({
        data: { table_fqns: resolvedFqns, sample_size: cfg.sample_size ?? 1000 },
      });
      const ok = result.data.submitted.length;
      const errs = result.data.errors.length;
      if (ok > 0) toast.success(t("runs.submittedRuns", { count: ok }));
      if (errs > 0) toast.error(t("runs.tablesFailedToSubmit", { count: errs }));
    } catch (err) {
      toast.error(t("runs.runFailed", { error: err instanceof Error ? err.message : t("common.unknownError") }));
    } finally {
      setIsRunningNow(false);
    }
  };

  if (loadingEntry) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!currentRunName) return <SelectRunState />;
  if (entryNotFound) return <RunNotFoundState runName={currentRunName} onAddRun={onAddRun} isAdmin={isAdmin} />;
  if (!scheduleEntry) return <EmptyState onAddRun={onAddRun} isAdmin={isAdmin} />;

  return (
    <RunEditor
      runName={currentRunName}
      yamlContent={yamlContent}
      setYamlContent={setYamlContent}
      isDirty={isDirty}
      setIsDirty={setIsDirty}
      onSave={handleSave}
      onReset={handleReset}
      onDelete={handleDelete}
      onRunNow={handleRunNow}
      isRunning={isRunningNow}
      isSaving={isSaving}
      isDeleting={isDeleting}
      isDeleteOpen={isDeleteOpen}
      setIsDeleteOpen={setIsDeleteOpen}
      isAdmin={isAdmin}
    />
  );
}

function EmptyState({ onAddRun, isAdmin }: { onAddRun: () => void; isAdmin: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-6">
        <CalendarClock className="h-8 w-8 text-primary" />
      </div>
      <h3 className="text-xl font-semibold mb-2">{t("runs.noScheduledRuns")}</h3>
      <p className="text-muted-foreground mb-6 max-w-md">
        {t("runs.noScheduledRunsDescription")}{isAdmin ? t("runs.noScheduledRunsAdminSuffix") : ""}
      </p>
      {isAdmin && (
      <Button onClick={onAddRun} className="gap-2">
        <Plus className="h-4 w-4" />
        {t("runs.createFirstSchedule")}
      </Button>
      )}
    </div>
  );
}

function RunNotFoundState({ runName, onAddRun, isAdmin }: { runName: string; onAddRun: () => void; isAdmin: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-destructive/10 flex items-center justify-center mb-6">
        <AlertCircle className="h-8 w-8 text-destructive" />
      </div>
      <h3 className="text-xl font-semibold mb-2">{t("runs.scheduleNotFound")}</h3>
      <p className="text-muted-foreground mb-6 max-w-md">
        {t("runs.scheduleNotFoundDescriptionPrefix")}
        <code className="px-1.5 py-0.5 bg-muted rounded text-sm font-mono">{runName}</code>
        {t("runs.scheduleNotFoundDescriptionSuffix")}
      </p>
      <div className="flex gap-3">
        <Button variant="outline" asChild>
          <Link to="/runs">{t("runs.viewAllSchedules")}</Link>
        </Button>
        {isAdmin && (
        <Button onClick={onAddRun} className="gap-2">
          <Plus className="h-4 w-4" />
          {t("runs.createNewSchedule")}
        </Button>
        )}
      </div>
    </div>
  );
}

function SelectRunState() {
  const { t } = useTranslation();
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
        <CalendarClock className="h-8 w-8 text-muted-foreground" />
      </div>
      <h3 className="text-lg font-medium text-muted-foreground">{t("runs.selectAScheduleTitle")}</h3>
      <p className="text-muted-foreground/70 text-sm mt-1">
        {t("runs.selectAScheduleDescription")}
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Run Editor with Form and YAML modes
// ---------------------------------------------------------------------------

interface RunEditorProps {
  runName: string;
  yamlContent: string;
  setYamlContent: (content: string) => void;
  isDirty: boolean;
  setIsDirty: (dirty: boolean) => void;
  onSave: () => void;
  onReset: () => void;
  onDelete: () => void;
  onRunNow?: () => void;
  isRunning?: boolean;
  isSaving: boolean;
  isDeleting: boolean;
  isDeleteOpen: boolean;
  setIsDeleteOpen: (open: boolean) => void;
  isAdmin: boolean;
}

function RunEditor({
  runName,
  yamlContent,
  setYamlContent,
  isDirty,
  setIsDirty,
  onSave,
  onReset,
  onDelete,
  onRunNow,
  isRunning,
  isSaving,
  isDeleting,
  isDeleteOpen,
  setIsDeleteOpen,
  isAdmin,
}: RunEditorProps) {
  const { t } = useTranslation();
  const isLocked = isSaving || isDeleting || !!isRunning || !isAdmin;

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between pb-4 border-b border-border/50">
        <div className="flex-1">
          <h2 className="text-2xl font-bold tracking-tight">{runName}</h2>
          <p className="text-muted-foreground text-sm mt-0.5">
            {isAdmin ? t("runs.editScheduleConfiguration") : t("runs.viewScheduleConfiguration")}
            {isDirty && isAdmin && <span className="text-amber-500 ml-2">{t("runs.unsavedChangesIndicator")}</span>}
          </p>
        </div>
        {isAdmin && (
        <div className="flex items-center gap-2">
            {onRunNow && (
              <Button variant="outline" size="sm" onClick={onRunNow} disabled={isDirty || isLocked} title={isDirty ? t("runs.saveBeforeRun") : t("runs.runNowTooltip")} className="gap-1.5">
                {isRunning ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {t("runs.runNow")}
              </Button>
            )}
            <Button variant="ghost" size="icon" onClick={onReset} disabled={!isDirty || isLocked} title={t("runs.resetChanges")}>
              <RotateCcw className="h-4 w-4" />
            </Button>
            <Button onClick={onSave} variant="default" size="icon" disabled={!isDirty || isLocked} title={t("runs.saveChanges")}>
              {isSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            </Button>
            <AlertDialog open={isDeleteOpen} onOpenChange={setIsDeleteOpen}>
              <AlertDialogTrigger asChild>
                <Button variant="destructive" size="icon" disabled={isDeleting || isLocked} title={t("runs.deleteSchedule")}>
                  <Trash2 className="h-4 w-4" />
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>{t("runs.deleteSchedule")}</AlertDialogTitle>
                  <AlertDialogDescription>
                    {t("runs.deleteScheduleConfirmPrefix")}
                    <span className="font-mono font-medium">{runName}</span>{t("runs.deleteScheduleConfirmSuffix")}
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel disabled={isDeleting}>{t("runs.cancel")}</AlertDialogCancel>
                  <AlertDialogAction
                    onClick={(e) => { e.preventDefault(); if (!isDeleting) onDelete(); }}
                    disabled={isDeleting}
                    className="bg-destructive text-foreground hover:bg-destructive/90"
                  >
                    {isDeleting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
                    {t("runs.delete")}
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        )}
      </div>
      <div className="flex-1 min-h-0 mt-4">
        <FormEditor yamlContent={yamlContent} setYamlContent={setYamlContent} setIsDirty={setIsDirty} isLocked={isLocked} />
      </div>
    </div>
  );
}

function FormEditor({
  yamlContent, setYamlContent, setIsDirty, isLocked,
}: { yamlContent: string; setYamlContent: (c: string) => void; setIsDirty: (d: boolean) => void; isLocked: boolean }) {
  const { t } = useTranslation();
  const [formData, setFormData] = useState<RunConfig | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [scheduleConfig, setScheduleConfig] = useState<ScheduleConfig>({ ...DEFAULT_SCHEDULE });
  const internalUpdate = useRef(false);

  const { data: rulesResp } = useListRules({ status: "approved" }, { query: {} });
  const approvedTables: RuleCatalogEntryOut[] = useMemo(() => {
    const rawRules = Array.isArray(rulesResp?.data)
      ? rulesResp.data.filter((r: RuleCatalogEntryOut) => r.status === "approved")
      : [];
    const byTable = new Map<string, RuleCatalogEntryOut>();
    for (const rule of rawRules) {
      const existing = byTable.get(rule.table_fqn);
      if (existing) {
        existing.checks = [...existing.checks, ...rule.checks];
      } else {
        byTable.set(rule.table_fqn, { ...rule, checks: [...rule.checks] });
      }
    }
    return Array.from(byTable.values());
  }, [rulesResp]);

  useEffect(() => {
    if (internalUpdate.current) {
      internalUpdate.current = false;
      return;
    }
    try {
      if (!yamlContent) {
        setFormData(null);
        return;
      }
      const parsed = yaml.load(yamlContent) as Record<string, any>;
      if (parsed && typeof parsed === "object") {
        setFormData(parsed as RunConfig);
        const schedFromYaml: Partial<ScheduleConfig> = {};
        if (parsed.frequency) schedFromYaml.frequency = parsed.frequency;
        if (parsed.hour != null) schedFromYaml.hour = parsed.hour;
        if (parsed.minute != null) schedFromYaml.minute = parsed.minute;
        if (parsed.day_of_week != null) schedFromYaml.day_of_week = parsed.day_of_week;
        if (parsed.day_of_month != null) schedFromYaml.day_of_month = parsed.day_of_month;
        if (parsed.scope_mode) schedFromYaml.scope_mode = parsed.scope_mode;
        if (parsed.scope_catalogs) schedFromYaml.scope_catalogs = parsed.scope_catalogs;
        if (parsed.scope_schemas) schedFromYaml.scope_schemas = parsed.scope_schemas;
        if (parsed.scope_tables) schedFromYaml.scope_tables = parsed.scope_tables;
        if (Array.isArray(parsed.scope_labels)) {
          // Tolerate both the canonical ``[{key, value}]`` form and a
          // ``["key=value"]`` shorthand that admins might hand-edit.
          schedFromYaml.scope_labels = parsed.scope_labels
            .map((entry: unknown) => {
              if (entry && typeof entry === "object" && !Array.isArray(entry)) {
                const e = entry as Record<string, unknown>;
                if (typeof e.key === "string") {
                  return { key: e.key, value: String(e.value ?? "") };
                }
              }
              if (typeof entry === "string") {
                const idx = entry.indexOf("=");
                if (idx < 0) return { key: entry, value: "" };
                return { key: entry.slice(0, idx), value: entry.slice(idx + 1) };
              }
              return null;
            })
            .filter((v: unknown): v is { key: string; value: string } => v !== null);
        }
        if (parsed.sample_size != null) schedFromYaml.sample_size = parsed.sample_size;
        if (parsed.cron_expression) schedFromYaml.cron_expression = parsed.cron_expression;
        setScheduleConfig({ ...DEFAULT_SCHEDULE, ...schedFromYaml });
        setParseError(null);
      } else {
        setFormData(null);
      }
    } catch (e) {
      setParseError(e instanceof Error ? e.message : t("runs.failedParseYaml"));
      setFormData(null);
    }
  }, [yamlContent, t]);

  const updateFormData = useCallback((updates: Partial<RunConfig>) => {
    setFormData((prev) => {
      if (!prev) return prev;
      const updated = { ...prev, ...updates };
      try {
        internalUpdate.current = true;
        const newYaml = yaml.dump(updated);
        setYamlContent(newYaml);
        setIsDirty(true);
      } catch {
        toast.error(t("runs.failedConvertYaml"));
      }
      return updated;
    });
  }, [setYamlContent, setIsDirty, t]);

  const handleScheduleChange = useCallback((cfg: ScheduleConfig) => {
    setScheduleConfig(cfg);
    setFormData((prev) => {
      if (!prev) return prev;
      const updated = { ...prev, ...cfg };
      try {
        internalUpdate.current = true;
        const newYaml = yaml.dump(updated);
        setYamlContent(newYaml);
        setIsDirty(true);
      } catch {
        toast.error(t("runs.failedConvertYaml"));
      }
      return updated;
    });
  }, [setYamlContent, setIsDirty, t]);

  if (parseError) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-destructive mx-auto mb-3" />
          <p className="text-destructive font-medium mb-1">{t("runs.invalidScheduleConfig")}</p>
          <p className="text-muted-foreground text-sm">{parseError}</p>
          <p className="text-muted-foreground text-xs mt-2">{t("runs.deleteRecreateHint")}</p>
        </div>
      </div>
    );
  }

  if (!formData) {
    return (
      <div className="h-full flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-4xl space-y-8 pr-4">
        <section>
          <h3 className="text-lg font-semibold mb-4">{t("runs.basicConfiguration")}</h3>
          <div className="space-y-4">
            <div className="grid gap-2">
              <Label htmlFor="name">{t("runs.name")}</Label>
              <Input id="name" value={formData.name || ""} onChange={(e) => updateFormData({ name: e.target.value })} disabled={isLocked} placeholder={t("runs.namePlaceholder")} />
            </div>
          </div>
        </section>

        <section>
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <CalendarClock className="h-5 w-5" /> {t("runs.schedule")}
          </h3>
          <p className="text-xs text-muted-foreground mb-3">
            {t("runs.scheduleSectionDescription")}
          </p>
          <ScheduleFrequencyPicker config={scheduleConfig} onChange={handleScheduleChange} disabled={isLocked} />
        </section>

        <section>
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Database className="h-5 w-5" /> {t("runs.ruleScope")}
          </h3>
          <p className="text-xs text-muted-foreground mb-3">
            {t("runs.ruleScopeDescriptionPrefix")}
            <code className="text-[10px] bg-muted px-1 py-0.5 rounded">dq_quality_rules</code>
            {t("runs.ruleScopeDescriptionSuffix")}
          </p>
          {approvedTables.length === 0 ? (
            <p className="text-sm text-muted-foreground">{t("runs.noApprovedRulesFound")}</p>
          ) : (
            <ScopePicker
              config={scheduleConfig}
              onChange={handleScheduleChange}
              approvedRules={approvedTables}
              disabled={isLocked}
            />
          )}
        </section>


      </div>
    </div>
  );
}

