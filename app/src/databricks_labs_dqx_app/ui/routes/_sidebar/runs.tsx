import {
  createFileRoute,
  Link,
  useNavigate,
  useParams,
} from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  useListRules,
  RunConfig,
  type RuleCatalogEntryOut,
  type User as UserType,
} from "@/lib/api";
import axios from "axios";
import {
  useBatchRunFromCatalog,
  useListValidationRuns,
  getListValidationRunsQueryKey,
  type ValidationRunSummaryOut,
} from "@/lib/api-custom";
import { useGetDryRunResults, type DryRunResultsOut } from "@/lib/api";
import { DryRunResults } from "@/components/DryRunResults";
import { CommentThread } from "@/components/CommentThread";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
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
  FileCode,
  AlertCircle,
  RotateCcw,
  Loader2,
  FormInput,
  Play,
  History,
  CheckCircle2,
  XCircle,
  Clock,
  User,
  Search,
  CalendarClock,
  Layers,
  Database,
  Table2,
  Zap,
  ChevronDown,
  ChevronRight,
  X,
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
import yaml from "js-yaml";
import { useQueryClient, QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useAIAssistant } from "@/components/AIAssistantProvider";
import { FadeIn } from "@/components/anim/FadeIn";
import { useActiveRuns, type ActiveRun } from "@/hooks/use-active-runs";
import { getDryRunStatus, type RunStatusOut } from "@/lib/api";
import { cancelDryRun } from "@/lib/api-custom";
import { CircleStop } from "lucide-react";
import { parseFqn, formatDateTime as formatDate } from "@/lib/format-utils";

export const Route = createFileRoute("/_sidebar/runs")({
  component: RunsPage,
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const _SQL_CHECK_PREFIX = "__sql_check__/";
function cleanFqn(fqn: string) {
  return fqn.startsWith(_SQL_CHECK_PREFIX) ? fqn.slice(_SQL_CHECK_PREFIX.length) : fqn;
}

function statusBadge(status: string | null) {
  switch (status) {
    case "SUCCESS":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          Success
        </Badge>
      );
    case "FAILED":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          Failed
        </Badge>
      );
    case "RUNNING":
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3 animate-spin" />
          Running
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status ?? "Unknown"}</Badge>;
  }
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

function cronPreview(cfg: ScheduleConfig): string {
  switch (cfg.frequency) {
    case "manual": return "Manual only (no automatic schedule)";
    case "hourly": return `Every hour at :${String(cfg.minute ?? 0).padStart(2, "0")} UTC`;
    case "daily": return `Daily at ${String(cfg.hour ?? 6).padStart(2, "0")}:${String(cfg.minute ?? 0).padStart(2, "0")} UTC`;
    case "weekly": {
      const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
      return `Every ${days[cfg.day_of_week ?? 1]} at ${String(cfg.hour ?? 6).padStart(2, "0")}:${String(cfg.minute ?? 0).padStart(2, "0")} UTC`;
    }
    case "monthly": return `Monthly on day ${cfg.day_of_month ?? 1} at ${String(cfg.hour ?? 6).padStart(2, "0")}:${String(cfg.minute ?? 0).padStart(2, "0")} UTC`;
    case "cron": return cfg.cron_expression ? `Cron: ${cfg.cron_expression}` : "Custom cron (not set)";
    default: return "";
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
  const update = (patch: Partial<ScheduleConfig>) => onChange({ ...config, ...patch });

  return (
    <div className="space-y-3">
      <div className="grid gap-2">
        <Label>Frequency</Label>
        <Select value={config.frequency} onValueChange={(v) => update({ frequency: v as ScheduleConfig["frequency"] })} disabled={disabled}>
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="manual">Manual only</SelectItem>
            <SelectItem value="hourly">Hourly</SelectItem>
            <SelectItem value="daily">Daily</SelectItem>
            <SelectItem value="weekly">Weekly</SelectItem>
            <SelectItem value="monthly">Monthly</SelectItem>
            <SelectItem value="cron">Custom cron</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {config.frequency === "cron" && (
        <div className="grid gap-2">
          <Label>Cron Expression</Label>
          <Input
            value={config.cron_expression || ""}
            onChange={(e) => update({ cron_expression: e.target.value })}
            disabled={disabled}
            placeholder="e.g., 0 6 * * MON-FRI"
            className="font-mono text-sm"
          />
          <p className="text-xs text-muted-foreground">Standard 5-field cron (minute hour day month weekday)</p>
        </div>
      )}

      {config.frequency === "hourly" && (
        <div className="grid gap-2">
          <Label>Minute</Label>
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
            <Label>Hour <span className="text-muted-foreground font-normal">(UTC)</span></Label>
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
            <Label>Minute</Label>
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
          <Label>Day of week</Label>
          <Select value={String(config.day_of_week ?? 1)} onValueChange={(v) => update({ day_of_week: Number(v) })} disabled={disabled}>
            <SelectTrigger className="w-32"><SelectValue /></SelectTrigger>
            <SelectContent>
              {["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"].map((d, i) => (
                <SelectItem key={i} value={String(i)}>{d}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {config.frequency === "monthly" && (
        <div className="grid gap-2">
          <Label>Day of month</Label>
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
          <Clock className="h-3 w-3" /> {cronPreview(config)}
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

  const matchedCount = useMemo(() => {
    return approvedRules.filter((r) => {
      const { catalog, schema } = parseFqn(r.table_fqn);
      switch (config.scope_mode) {
        case "all": return true;
        case "catalog": return (config.scope_catalogs ?? []).includes(catalog);
        case "schema": return (config.scope_schemas ?? []).includes(`${catalog}.${schema}`);
        case "tables": return (config.scope_tables ?? []).includes(r.table_fqn);
        default: return true;
      }
    }).length;
  }, [approvedRules, config]);

  const toggleInList = (list: string[], item: string): string[] => {
    return list.includes(item) ? list.filter((x) => x !== item) : [...list, item];
  };

  return (
    <div className="space-y-3">
      <div className="grid gap-2">
        <Label>Scope</Label>
        <Select value={config.scope_mode} onValueChange={(v) => update({ scope_mode: v as ScheduleConfig["scope_mode"] })} disabled={disabled}>
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all"><span className="flex items-center gap-1.5"><Database className="h-3 w-3" /> All approved rules</span></SelectItem>
            <SelectItem value="catalog"><span className="flex items-center gap-1.5"><Database className="h-3 w-3" /> By catalog</span></SelectItem>
            <SelectItem value="schema"><span className="flex items-center gap-1.5"><Layers className="h-3 w-3" /> By schema</span></SelectItem>
            <SelectItem value="tables"><span className="flex items-center gap-1.5"><Table2 className="h-3 w-3" /> Specific tables</span></SelectItem>
          </SelectContent>
        </Select>
      </div>

      {config.scope_mode === "catalog" && (
        <div className="border rounded-lg overflow-hidden max-h-44 overflow-y-auto">
          {allCatalogs.length === 0 ? (
            <p className="p-3 text-xs text-muted-foreground">No catalogs found in approved rules.</p>
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
            <p className="p-3 text-xs text-muted-foreground">No schemas found in approved rules.</p>
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
            <p className="p-3 text-xs text-muted-foreground">No approved rules available.</p>
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
                  {(config.scope_tables ?? []).length > 0 ? `${(config.scope_tables ?? []).length} of ${approvedRules.length} selected` : "Select all"}
                </span>
              </div>
              {approvedRules.map((rule) => (
                <div
                  key={rule.table_fqn}
                  className={cn("flex items-center gap-3 px-2.5 py-2 hover:bg-muted/20 cursor-pointer border-b last:border-b-0", (config.scope_tables ?? []).includes(rule.table_fqn) && "bg-primary/5")}
                  onClick={() => !disabled && update({ scope_tables: toggleInList(config.scope_tables ?? [], rule.table_fqn) })}
                >
                  <Checkbox checked={(config.scope_tables ?? []).includes(rule.table_fqn)} onCheckedChange={() => update({ scope_tables: toggleInList(config.scope_tables ?? [], rule.table_fqn) })} disabled={disabled} />
                  <span className="font-mono text-xs flex-1 truncate">{rule.display_name || rule.table_fqn}</span>
                  <Badge variant="secondary" className="text-[10px]">{rule.checks.length} rule{rule.checks.length !== 1 ? "s" : ""}</Badge>
                </div>
              ))}
            </>
          )}
        </div>
      )}

      <p className="text-xs text-muted-foreground">
        {matchedCount} of {approvedRules.length} approved rule set{approvedRules.length !== 1 ? "s" : ""} matched
      </p>

      <div className="grid gap-2">
        <Label>Row scope</Label>
        <Select
          value={config.sample_size === 0 ? "all" : "sample"}
          onValueChange={(v) => update({ sample_size: v === "all" ? 0 : 1000 })}
          disabled={disabled}
        >
          <SelectTrigger className="w-48"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All rows</SelectItem>
            <SelectItem value="sample">Sample rows</SelectItem>
          </SelectContent>
        </Select>
        {(config.sample_size ?? 1000) > 0 && (
          <div className="flex items-center gap-2">
            <Label className="text-xs text-muted-foreground whitespace-nowrap">Sample size per table</Label>
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
  return approvedRules
    .filter((r) => {
      const { catalog, schema } = parseFqn(r.table_fqn);
      switch (cfg.scope_mode) {
        case "all": return true;
        case "catalog": return (cfg.scope_catalogs ?? []).includes(catalog);
        case "schema": return (cfg.scope_schemas ?? []).includes(`${catalog}.${schema}`);
        case "tables": return (cfg.scope_tables ?? []).includes(r.table_fqn);
        default: return true;
      }
    })
    .map((r) => r.table_fqn);
}

// ---------------------------------------------------------------------------
// Main page — three top-level tabs: Execute, Schedules, History
// ---------------------------------------------------------------------------

function RunsPage() {
  const navigate = useNavigate();
  const params = useParams({ strict: false }) as { runName?: string };
  const currentRunName = params.runName;
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [isDeletingRun, setIsDeletingRun] = useState(false);
  const queryClient = useQueryClient();

  const [scheduleNames, setScheduleNames] = useState<string[]>([]);

  useEffect(() => {
    fetchSchedules().then((entries) => {
      setScheduleNames(entries.map((e) => e.schedule_name));
    }).catch(() => {});
  }, []);

  const handleCreateRun = async (name: string) => {
    try {
      await saveScheduleApi(name, { ...DEFAULT_SCHEDULE });
      await queryClient.refetchQueries({ queryKey: [...SCHEDULES_KEY] });
      setScheduleNames((prev) => [...prev, name]);
      toast.success(`Schedule "${name}" created`);
      navigate({ to: "/runs/$runName", params: { runName: name } });
      setIsCreateOpen(false);
    } catch (error) {
      toast.error("Failed to create new schedule");
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
        items={currentRunName ? [{ label: "Run Rules", to: "/runs" }] : []}
        page={currentRunName || "Run Rules"}
      />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col mt-4 overflow-hidden">
        <div className="flex items-center justify-between shrink-0 mb-4">
          <TabsList>
            <TabsTrigger value="execute" className="gap-2">
              <Zap className="h-4 w-4" />
              Execute
            </TabsTrigger>
            <TabsTrigger value="schedules" className="gap-2">
              <CalendarClock className="h-4 w-4" />
              Schedules
            </TabsTrigger>
            <TabsTrigger value="history" className="gap-2">
              <History className="h-4 w-4" />
              History
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="execute" className="flex-1 overflow-hidden mt-0">
          <QueryErrorResetBoundary>
            {({ reset }) => (
              <ErrorBoundary onReset={reset} fallbackRender={ExecuteTabError}>
                <Suspense fallback={<ExecuteTabSkeleton />}>
                  <ExecuteTab onGoToHistory={() => setActiveTab("history")} />
                </Suspense>
              </ErrorBoundary>
            )}
          </QueryErrorResetBoundary>
        </TabsContent>

        <TabsContent value="schedules" className="flex-1 overflow-hidden mt-0">
          <div className="flex flex-1 gap-6 overflow-hidden h-full">
            <aside className="w-72 shrink-0 flex flex-col border-r border-border/50 pr-4 overflow-hidden">
              <div className="flex items-center justify-between mb-4 shrink-0">
                <h2 className="font-semibold text-lg text-foreground">
                  Scheduled Runs
                </h2>
                <Button
                  variant="outline"
                  size="icon"
                  onClick={() => setIsCreateOpen(true)}
                  title="Add New Schedule"
                  className="h-9 w-9"
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
              <div className="flex-1 overflow-y-auto space-y-1">
                <QueryErrorResetBoundary>
                  {({ reset }) => (
                    <ErrorBoundary onReset={reset} fallbackRender={RunsListError}>
                      <Suspense fallback={<RunsListSkeleton />}>
                        <RunsSidebarList
                          currentRunName={currentRunName}
                          isDeleting={isDeletingRun}
                        />
                      </Suspense>
                    </ErrorBoundary>
                  )}
                </QueryErrorResetBoundary>
              </div>
            </aside>
            <main className="flex-1 overflow-hidden flex flex-col">
              <QueryErrorResetBoundary>
                {({ reset }) => (
                  <ErrorBoundary onReset={reset} fallbackRender={RunEditorError}>
                    <Suspense fallback={<RunEditorSkeleton />}>
                      <RunEditorContainer
                        currentRunName={currentRunName}
                        onAddRun={() => setIsCreateOpen(true)}
                        onDeletingChange={setIsDeletingRun}
                      />
                    </Suspense>
                  </ErrorBoundary>
                )}
              </QueryErrorResetBoundary>
            </main>
          </div>
        </TabsContent>

        <TabsContent value="history" className="flex-1 overflow-hidden mt-0">
          <QueryErrorResetBoundary>
            {({ reset }) => (
              <ErrorBoundary onReset={reset} fallbackRender={RunHistoryError}>
                <Suspense fallback={<RunHistorySkeleton />}>
                  <RunHistoryTab />
                </Suspense>
              </ErrorBoundary>
            )}
          </QueryErrorResetBoundary>
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
  const [isRunning, setIsRunning] = useState(false);
  const [runNotification, setRunNotification] = useState<RunNotification | null>(null);

  const { activeRuns, addRuns, removeRun, clearAll: clearActiveRuns } = useActiveRuns();
  const runningFqns = useMemo(
    () => new Set(activeRuns.map((r) => r.table_fqn)),
    [activeRuns],
  );
  const batchRun = useBatchRunFromCatalog();
  const queryClient = useQueryClient();

  const filteredRules = useMemo(() => {
    if (!searchQuery) return approvedRules;
    const q = searchQuery.toLowerCase();
    return approvedRules.filter((r) => (r.display_name || r.table_fqn).toLowerCase().includes(q));
  }, [approvedRules, searchQuery]);

  const grouped = useMemo(() => {
    const groups = new Map<string, RuleCatalogEntryOut[]>();

    for (const rule of filteredRules) {
      const isSqlCheck = rule.table_fqn.startsWith(_SQL_CHECK_PREFIX);
      let key: string;
      if (isSqlCheck) {
        key = "Cross-table rules";
      } else {
        const { catalog, schema } = parseFqn(rule.table_fqn);
        switch (groupBy) {
          case "catalog":
            key = catalog || "Unknown";
            break;
          case "schema":
            key = `${catalog}.${schema}` || "Unknown";
            break;
          default:
            key = "All";
        }
      }
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(rule);
    }

    return new Map([...groups.entries()].sort(([a], [b]) => a.localeCompare(b)));
  }, [filteredRules, groupBy]);

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
      toast.error(`Batch run failed: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setIsRunning(false);
    }
  };

  if (rulesLoading) return <ExecuteTabSkeleton />;

  return (
    <div className="space-y-6 h-full overflow-y-auto">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold tracking-tight">Execute Rules</h2>
          <p className="text-muted-foreground text-sm">
            Select approved rules and trigger manual validation runs.
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
                  Started {runNotification.count} validation run
                  {runNotification.count !== 1 ? "s" : ""}
                  {runNotification.errors > 0 && (
                    <span className="text-destructive ml-1">
                      ({runNotification.errors} failed to submit)
                    </span>
                  )}
                </span>
              </>
            ) : (
              <>
                <XCircle className="h-4 w-4 text-destructive shrink-0" />
                <span className="text-destructive">
                  All {runNotification.errors} table{runNotification.errors !== 1 ? "s" : ""} failed to submit
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
                View in History
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
                No approved rules
              </h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                Approve rules in Drafts & review first, then come back here to run them.
              </p>
              <Button variant="outline" className="mt-4" asChild>
                <Link to="/rules/drafts">Go to Drafts & review</Link>
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
                    Table Selection
                  </CardTitle>
                  <CardDescription>
                    {approvedRules.length} approved rule set{approvedRules.length !== 1 ? "s" : ""} available
                    {selectedTables.size > 0 && (
                      <span className="text-primary font-medium ml-1">
                        · {selectedTables.size} selected
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
                        <SelectItem value="all">All rows</SelectItem>
                        <SelectItem value="sample">Sample rows</SelectItem>
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
                      ? "Running..."
                      : `Run ${selectedTables.size > 0 ? selectedTables.size : ""} selected`}
                  </Button>
                </div>
              </div>

              <div className="flex items-center gap-2 flex-wrap pt-3 border-t mt-3">
                <div className="flex items-center gap-1.5">
                  <Layers className="h-3.5 w-3.5 text-muted-foreground" />
                  <span className="text-xs text-muted-foreground">Group by:</span>
                  <Select value={groupBy} onValueChange={(v) => setGroupBy(v as GroupMode)}>
                    <SelectTrigger className="w-[120px] h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="catalog" className="text-xs">
                        <span className="flex items-center gap-1.5"><Database className="h-3 w-3" /> Catalog</span>
                      </SelectItem>
                      <SelectItem value="schema" className="text-xs">
                        <span className="flex items-center gap-1.5"><Layers className="h-3 w-3" /> Schema</span>
                      </SelectItem>
                      <SelectItem value="none" className="text-xs">
                        <span className="flex items-center gap-1.5"><Table2 className="h-3 w-3" /> Flat list</span>
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="relative flex-1 max-w-xs">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Search tables..."
                    className="h-8 pl-8 text-xs"
                  />
                </div>

                <div className="flex items-center gap-1.5 ml-auto">
                  <Button variant="ghost" size="sm" className="h-8 text-xs" onClick={selectAll}>
                    Select all
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 text-xs"
                    onClick={clearAll}
                    disabled={selectedTables.size === 0}
                  >
                    Clear
                  </Button>
                </div>
              </div>
            </CardHeader>

            <CardContent>
              {filteredRules.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground text-sm">
                  No rules match your search.
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
                              {rules.length} table{rules.length !== 1 ? "s" : ""}
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
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2 text-base">
              <Loader2 className="h-4 w-4 animate-spin text-amber-500" />
              Active Runs
              <Badge variant="secondary" className="text-xs">{runs.length}</Badge>
            </CardTitle>
            <CardDescription>
              These validation runs are currently in progress. Status updates automatically.
            </CardDescription>
          </div>
          <Button variant="ghost" size="sm" className="text-xs text-muted-foreground" onClick={onDismissAll}>
            Dismiss all
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                <th className="text-left p-3 font-medium">Table</th>
                <th className="text-left p-3 font-medium">Run ID</th>
                <th className="text-left p-3 font-medium">Status</th>
                <th className="text-left p-3 font-medium">Elapsed</th>
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
        const resp = await getDryRunStatus(run.run_id);
        if (cancelled) return;
        setStatus(resp.data);
        setPollError(false);

        if (TERMINAL_STATES.has(resp.data.state)) {
          const tableName = cleanFqn(run.table_fqn).split(".").pop();
          if (resp.data.state === "TERMINATED" && resp.data.result_state === "SUCCESS") {
            toast.success(`Run for ${tableName} completed successfully`);
          } else if (resp.data.state === "INTERNAL_ERROR") {
            toast.error(`Run for ${tableName} failed with internal error`);
          } else {
            toast.info(`Run for ${tableName} finished (${resp.data.result_state ?? resp.data.state})`);
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
  }, [run.run_id, run.job_run_id, run.view_fqn, run.table_fqn]);

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
      const tableName = cleanFqn(run.table_fqn).split(".").pop();
      toast.info(`Run for ${tableName} canceled`);
      onCompleteRef.current(run.run_id);
    } catch {
      toast.error("Failed to cancel run");
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
            Polling error
          </Badge>
        ) : stateLabel === "INTERNAL_ERROR" ? (
          <Badge variant="outline" className="gap-1 border-red-500 text-red-600 text-xs">
            <XCircle className="h-3 w-3" />
            Internal Error
          </Badge>
        ) : stateLabel === "RUNNING" ? (
          <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600 text-xs">
            <Loader2 className="h-3 w-3 animate-spin" />
            Running
          </Badge>
        ) : stateLabel === "PENDING" ? (
          <Badge variant="outline" className="gap-1 border-blue-500 text-blue-600 text-xs">
            <Clock className="h-3 w-3" />
            Pending
          </Badge>
        ) : stateLabel === "TERMINATED" ? (
          <Badge variant="outline" className="gap-1 border-green-500 text-green-600 text-xs">
            <CheckCircle2 className="h-3 w-3" />
            {status?.result_state ?? "Done"}
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
              title="Stop run"
            >
              {isCancelling ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : (
                <CircleStop className="h-3.5 w-3.5" />
              )}
              <span className="text-xs">Stop</span>
            </Button>
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0"
            onClick={() => onComplete(run.run_id)}
            title="Dismiss"
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
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b bg-muted/30">
          <th className="w-10 p-3" />
          <th className="text-left p-3 font-medium">Table</th>
          <th className="text-right p-3 font-medium">Rules</th>
          <th className="text-left p-3 font-medium">Status</th>
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
              <td className="p-3 font-mono text-xs">{rule.display_name || rule.table_fqn}</td>
              <td className="p-3 text-right tabular-nums">{rule.checks.length}</td>
              <td className="p-3">
                <div className="flex items-center gap-1.5">
                  <Badge variant="outline" className="gap-1 border-green-500 text-green-600 text-xs">
                    <CheckCircle2 className="h-3 w-3" />
                    Approved
                  </Badge>
                  {isTableRunning && (
                    <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600 text-xs">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Running
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
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">Failed to load rules</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2 mt-2">
        <RotateCcw className="h-3 w-3" />
        Retry
      </Button>
    </div>
  );
}

// ===========================================================================
// Run History Tab
// ===========================================================================

function RunHistoryTab() {
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [tableSearch, setTableSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [runTypeFilter, setRunTypeFilter] = useState("all");
  const [invalidOnly, setInvalidOnly] = useState(false);
  const [myRunsOnly, setMyRunsOnly] = useState(false);
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  const { data: runsResp, isLoading, error, refetch } = useListValidationRuns();
  const { data: rulesResp } = useListRules({ status: "approved" }, { query: {} });

  const rulesByTable = useMemo(() => {
    const map = new Map<string, Record<string, unknown>[]>();
    const rules = Array.isArray(rulesResp?.data) ? rulesResp.data : [];
    for (const rule of rules) {
      const existing = map.get(rule.table_fqn);
      if (existing) {
        existing.push(...rule.checks);
      } else {
        map.set(rule.table_fqn, [...rule.checks]);
      }
    }
    return map;
  }, [rulesResp]);

  const allRuns: ValidationRunSummaryOut[] = useMemo(() => {
    const raw = Array.isArray(runsResp?.data) ? runsResp.data : [];
    return raw.map((run) => {
      if (run.checks && run.checks.length > 0) return run;
      const fallback = rulesByTable.get(run.source_table_fqn);
      if (fallback && fallback.length > 0) {
        return { ...run, checks: fallback };
      }
      return run;
    });
  }, [runsResp, rulesByTable]);

  const { catalogs, schemasByCatalog } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    for (const run of allRuns) {
      const { catalog, schema } = parseFqn(run.source_table_fqn);
      if (catalog) {
        catalogSet.add(catalog);
        if (!schemaMap.has(catalog)) schemaMap.set(catalog, new Set());
        if (schema) schemaMap.get(catalog)!.add(schema);
      }
    }
    return {
      catalogs: Array.from(catalogSet).sort(),
      schemasByCatalog: Object.fromEntries(
        Array.from(schemaMap.entries()).map(([cat, schemas]) => [cat, Array.from(schemas).sort()]),
      ),
    };
  }, [allRuns]);

  const runs = useMemo(() => {
    return allRuns.filter((run) => {
      const { catalog, schema, table } = parseFqn(run.source_table_fqn);
      if (catalogFilter !== "all" && catalog !== catalogFilter) return false;
      if (schemaFilter !== "all" && schema !== schemaFilter) return false;
      if (tableSearch && !table.toLowerCase().includes(tableSearch.toLowerCase()) && !run.source_table_fqn.toLowerCase().includes(tableSearch.toLowerCase())) return false;
      if (statusFilter !== "all" && run.status !== statusFilter) return false;
      if (runTypeFilter !== "all" && (run.run_type ?? "dryrun") !== runTypeFilter) return false;
      if (invalidOnly && !(run.invalid_rows != null && run.invalid_rows > 0)) return false;
      if (myRunsOnly && currentUserEmail && run.requesting_user !== currentUserEmail) return false;
      return true;
    });
  }, [allRuns, catalogFilter, schemaFilter, tableSearch, statusFilter, runTypeFilter, invalidOnly, myRunsOnly, currentUserEmail]);

  const availableSchemas = catalogFilter !== "all" ? schemasByCatalog[catalogFilter] || [] : [];

  const handleCatalogChange = (value: string) => {
    setCatalogFilter(value);
    setSchemaFilter("all");
  };

  const hasActiveFilters = catalogFilter !== "all" || schemaFilter !== "all" || tableSearch !== "" || statusFilter !== "all" || runTypeFilter !== "all" || invalidOnly || myRunsOnly;

  return (
    <div className="space-y-4 h-full overflow-y-auto">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold tracking-tight">Run History</h2>
          <p className="text-muted-foreground text-sm">
            View past rule validation results.
          </p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()} className="gap-1.5 text-xs">
          <RotateCcw className="h-3.5 w-3.5" />
          Refresh
        </Button>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2 text-base">
                <History className="h-4 w-4" />
                Validation runs
              </CardTitle>
              <CardDescription>
                {isLoading
                  ? "Loading..."
                  : `${runs.length} run${runs.length !== 1 ? "s" : ""}${
                      runs.length !== allRuns.length ? ` (filtered from ${allRuns.length})` : ""
                    }`}
              </CardDescription>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap pt-2">
            <Select value={catalogFilter} onValueChange={handleCatalogChange}>
              <SelectTrigger className="w-[160px]">
                <SelectValue placeholder="All Catalogs" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Catalogs</SelectItem>
                {catalogs.map((cat) => (
                  <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={schemaFilter} onValueChange={setSchemaFilter} disabled={catalogFilter === "all"}>
              <SelectTrigger className="w-[160px]">
                <SelectValue placeholder="All Schemas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Schemas</SelectItem>
                {availableSchemas.map((sch) => (
                  <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                value={tableSearch}
                onChange={(e) => setTableSearch(e.target.value)}
                placeholder="Search table..."
                className="w-[180px] h-9 pl-8 text-sm"
              />
            </div>

            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-[140px]">
                <SelectValue placeholder="All Statuses" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Statuses</SelectItem>
                <SelectItem value="SUCCESS">Success</SelectItem>
                <SelectItem value="FAILED">Failed</SelectItem>
                <SelectItem value="RUNNING">Running</SelectItem>
              </SelectContent>
            </Select>

            <Select value={runTypeFilter} onValueChange={setRunTypeFilter}>
              <SelectTrigger className="w-[140px]">
                <SelectValue placeholder="All Types" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Types</SelectItem>
                <SelectItem value="dryrun">Dry Run</SelectItem>
                <SelectItem value="scheduled">Scheduled</SelectItem>
              </SelectContent>
            </Select>

            <Button
              variant={invalidOnly ? "default" : "outline"}
              size="sm"
              className="h-9 gap-1.5 text-xs"
              onClick={() => setInvalidOnly((prev) => !prev)}
            >
              <AlertCircle className="h-3.5 w-3.5" />
              Has invalid
            </Button>

            <Button
              variant={myRunsOnly ? "default" : "outline"}
              size="sm"
              className="h-9 gap-1.5 text-xs"
              onClick={() => setMyRunsOnly((prev) => !prev)}
            >
              <User className="h-3.5 w-3.5" />
              My runs
            </Button>

            {hasActiveFilters && (
              <Button
                variant="ghost"
                size="sm"
                className="h-9 text-xs"
                onClick={() => {
                  setCatalogFilter("all");
                  setSchemaFilter("all");
                  setTableSearch("");
                  setStatusFilter("all");
                  setRunTypeFilter("all");
                  setInvalidOnly(false);
                  setMyRunsOnly(false);
                }}
              >
                Clear filters
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {isLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} className="h-14 w-full" />)}
            </div>
          )}

          {error && (
            <p className="text-destructive text-sm">Failed to load run history: {(error as Error).message}</p>
          )}

          {!isLoading && !error && runs.length > 0 && (
            <FadeIn duration={0.3}>
              <div className="border rounded-lg overflow-x-auto">
                <table className="w-full text-sm min-w-[900px]">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="w-8 p-3"></th>
                      <th className="text-left p-3 font-medium">Table</th>
                      <th className="text-left p-3 font-medium">Type</th>
                      <th className="text-left p-3 font-medium">Status</th>
                      <th className="text-left p-3 font-medium">Rules</th>
                      <th className="text-left p-3 font-medium">Requested by</th>
                      <th className="text-right p-3 font-medium">Total</th>
                      <th className="text-right p-3 font-medium">Valid</th>
                      <th className="text-right p-3 font-medium">Invalid</th>
                      <th className="text-left p-3 font-medium">Run date</th>
                    </tr>
                  </thead>
                  <tbody>
                    {runs.map((run) => {
                      const invalidPct =
                        run.total_rows && run.invalid_rows
                          ? ((run.invalid_rows / run.total_rows) * 100).toFixed(1)
                          : null;
                      const isExpanded = expandedRunId === run.run_id;
                      return (
                        <RunHistoryRow
                          key={run.run_id}
                          run={run}
                          invalidPct={invalidPct}
                          isExpanded={isExpanded}
                          onToggle={() => setExpandedRunId(isExpanded ? null : run.run_id)}
                        />
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </FadeIn>
          )}

          {!isLoading && !error && runs.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-center">
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
                <History className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-muted-foreground">
                {hasActiveFilters ? "No matching runs" : "No runs yet"}
              </h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                {hasActiveFilters
                  ? "Try adjusting your filters to find runs."
                  : "Execute approved rules from the Execute tab to see results here."}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

// ===========================================================================
// Skeletons / Error states
// ===========================================================================

function RunHistorySkeleton() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-7 w-32" />
          <Skeleton className="h-4 w-64" />
        </div>
        <Skeleton className="h-9 w-28" />
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Run History Row with expandable detail
// ──────────────────────────────────────────────────────────────────────────────

function RunHistoryRow({
  run,
  invalidPct,
  isExpanded,
  onToggle,
}: {
  run: ValidationRunSummaryOut;
  invalidPct: string | null;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const { data: resultsResp, isLoading: isLoadingResults } = useGetDryRunResults(
    run.run_id,
    { query: { enabled: isExpanded && run.status === "SUCCESS" } },
  );
  const results: DryRunResultsOut | null = isExpanded && resultsResp?.data ? resultsResp.data : null;

  return (
    <>
      <tr
        className={cn(
          "border-b hover:bg-muted/30 transition-colors cursor-pointer",
          isExpanded && "bg-muted/20",
        )}
        onClick={onToggle}
      >
        <td className="p-3 w-8">
          {isExpanded ? (
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-4 w-4 text-muted-foreground" />
          )}
        </td>
        <td className="p-3 font-mono text-xs">{cleanFqn(run.source_table_fqn)}</td>
        <td className="p-3">
          <Badge variant={(run.run_type ?? "dryrun") === "scheduled" ? "default" : "outline"} className="text-[10px]">
            {(run.run_type ?? "dryrun") === "scheduled" ? "Scheduled" : "Dry Run"}
          </Badge>
        </td>
        <td className="p-3">
          <div className="flex flex-col gap-0.5">
            {statusBadge(run.status)}
            {run.status === "CANCELED" && run.canceled_by && (
              <span className="text-[10px] text-muted-foreground" title={`Canceled by ${run.canceled_by}`}>
                by {run.canceled_by.split("@")[0]}
              </span>
            )}
          </div>
        </td>
        <td className="p-3">
          {run.checks && run.checks.length > 0 ? (() => {
            const labels = run.checks.map((c: Record<string, unknown>) => {
              const check = c.check as Record<string, unknown> | undefined;
              const fn = check?.function as string | undefined;
              const args = check?.arguments as Record<string, unknown> | undefined;
              if (!fn) return c.name as string ?? "check";
              const col = args?.column as string | undefined;
              return col ? `${fn}(${col})` : fn;
            });
            const MAX_SHOWN = 3;
            const shown = labels.slice(0, MAX_SHOWN);
            const remaining = labels.length - MAX_SHOWN;
            return (
              <div className="flex flex-col gap-0.5 max-w-[240px]" title={labels.join("\n")}>
                {shown.map((label, i) => (
                  <span key={i} className="font-mono text-[11px] text-muted-foreground truncate">{label}</span>
                ))}
                {remaining > 0 && (
                  <span className="text-[10px] text-muted-foreground/70">+{remaining} more</span>
                )}
              </div>
            );
          })() : (
            <span className="text-xs text-muted-foreground">—</span>
          )}
        </td>
        <td className="p-3 text-xs text-muted-foreground truncate max-w-[180px]" title={run.requesting_user ?? ""}>
          {run.requesting_user ?? "—"}
        </td>
        <td className="p-3 text-right tabular-nums">{run.total_rows?.toLocaleString() ?? "—"}</td>
        <td className="p-3 text-right tabular-nums text-green-600">
          {run.valid_rows?.toLocaleString() ?? "—"}
        </td>
        <td className="p-3 text-right tabular-nums">
          {run.invalid_rows != null ? (
            <span className={run.invalid_rows > 0 ? "text-red-600 font-medium" : ""}>
              {run.invalid_rows.toLocaleString()}
              {invalidPct && run.invalid_rows > 0 && (
                <span className="text-muted-foreground font-normal ml-1">({invalidPct}%)</span>
              )}
            </span>
          ) : (
            "—"
          )}
        </td>
        <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={run.created_at ?? ""}>
          {formatDate(run.created_at)}
        </td>
      </tr>
      {isExpanded && (
        <tr>
          <td colSpan={10} className="p-0">
            <div className="border-t bg-muted/10 p-4 space-y-4">
              {run.status !== "SUCCESS" && (
                <div className="text-sm text-muted-foreground">
                  Detailed results are available for completed (SUCCESS) runs.
                </div>
              )}
              {run.status === "SUCCESS" && isLoadingResults && (
                <div className="flex items-center gap-2 text-muted-foreground text-sm py-4">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading results...
                </div>
              )}
              {results && <DryRunResults result={results} />}
              <CommentThread entityType="run" entityId={run.run_id} />
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function RunHistoryError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">Failed to load run history</p>
      <p className="text-muted-foreground/70 text-xs mb-3">
        The validation runs table may not exist yet. Run some rules first.
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        Retry
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
  const [name, setName] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (open) {
      setName("");
      setIsSubmitting(false);
    }
  }, [open]);

  const isConflict = existingNames.includes(name);
  const isValid = name.trim().length > 0 && !isConflict;

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
          <DialogTitle>Create Schedule</DialogTitle>
          <DialogDescription>
            Enter a unique name for the new scheduled run configuration.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit}>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className={cn(isConflict && "border-destructive focus-visible:ring-destructive")}
                placeholder="e.g. daily_sales_check"
                autoFocus
                autoComplete="off"
              />
              {isConflict && (
                <p className="text-xs text-destructive">This schedule name already exists.</p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button type="submit" disabled={!isValid || isSubmitting}>
              {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
              Create
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
  return (
    <div className="flex flex-col items-center justify-center py-8 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">Failed to load schedules</p>
      <p className="text-muted-foreground/70 text-xs mb-3">
        Please check your configuration settings
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        Retry
      </Button>
    </div>
  );
}

function RunEditorError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center">
      <AlertCircle className="h-16 w-16 text-destructive/30 mb-4" />
      <h3 className="text-lg font-semibold mb-2">Failed to load schedule editor</h3>
      <p className="text-muted-foreground text-sm mb-4">
        Please check your configuration settings
      </p>
      <Button variant="outline" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-4 w-4" />
        Retry
      </Button>
    </div>
  );
}

function RunsSidebarList({
  currentRunName,
  isDeleting,
}: {
  currentRunName?: string;
  isDeleting?: boolean;
}) {
  const [schedules, setSchedules] = useState<ScheduleConfigEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchSchedules()
      .then(setSchedules)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [isDeleting]);

  if (isDeleting || loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (schedules.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-8 text-center">
        <CalendarClock className="h-12 w-12 text-muted-foreground/30 mb-3" />
        <p className="text-muted-foreground text-sm mb-1">No schedules configured</p>
        <p className="text-muted-foreground/70 text-xs">
          Click the + button to create your first schedule
        </p>
      </div>
    );
  }

  return (
    <>
      {schedules.map((sched) => (
        <div
          key={sched.schedule_name}
          className={cn(
            "group flex items-center gap-2 rounded-lg transition-all duration-200",
            currentRunName === sched.schedule_name
              ? "bg-primary/10 ring-1 ring-primary/20"
              : "hover:bg-muted/50",
          )}
        >
          <Link
            to="/runs/$runName"
            params={{ runName: sched.schedule_name }}
            className={cn(
              "flex-1 flex items-center gap-3 px-3 py-2.5 text-sm font-medium",
              currentRunName === sched.schedule_name
                ? "text-primary"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            <CalendarClock className="h-4 w-4 shrink-0" />
            <span className="truncate">{sched.schedule_name}</span>
          </Link>
        </div>
      ))}
    </>
  );
}

function RunEditorContainer({
  currentRunName,
  onAddRun,
  onDeletingChange,
}: {
  currentRunName?: string;
  onAddRun: () => void;
  onDeletingChange: (isDeleting: boolean) => void;
}) {
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
        toast.error("Error parsing schedule configuration");
      }
    } else {
      setYamlContent("");
      setIsDirty(false);
    }
  }, [scheduleEntry]);

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
      toast.success("Schedule configuration saved successfully");
      setIsDirty(false);
      if (schedName !== currentRunName) {
        navigate({ to: "/runs/$runName", params: { runName: schedName } });
      }
      await queryClient.refetchQueries({ queryKey: [...SCHEDULES_KEY] });
    } catch (error) {
      console.error("Save error", error);
      const message = error instanceof Error ? error.message : "Check YAML syntax or validation errors.";
      toast.error(`Failed to save: ${message}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!currentRunName) return;
    setIsDeleteOpen(false);
    setIsDeleting(true);

    navigate({ to: "/runs" });

    try {
      await deleteScheduleApi(currentRunName);
      await queryClient.refetchQueries({ queryKey: [...SCHEDULES_KEY] });
      toast.success(`Schedule "${currentRunName}" deleted`);
    } catch (error) {
      console.error("Delete error", error);
      toast.error("Failed to delete schedule");
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
        toast.info("Changes discarded");
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
        toast.error("No rule sets matched the schedule scope. Check that approved rules exist for the selected scope.");
        return;
      }
      const result = await batchRun.mutateAsync({
        data: { table_fqns: resolvedFqns, sample_size: cfg.sample_size ?? 1000 },
      });
      const ok = result.data.submitted.length;
      const errs = result.data.errors.length;
      if (ok > 0) toast.success(`Submitted ${ok} rule run${ok !== 1 ? "s" : ""}`);
      if (errs > 0) toast.error(`${errs} table${errs !== 1 ? "s" : ""} failed to submit`);
    } catch (err) {
      toast.error(`Run failed: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setIsRunningNow(false);
    }
  };

  if (isDeleting || loadingEntry) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!currentRunName) return <SelectRunState />;
  if (entryNotFound) return <RunNotFoundState runName={currentRunName} onAddRun={onAddRun} />;
  if (!scheduleEntry) return <EmptyState onAddRun={onAddRun} />;

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
    />
  );
}

function EmptyState({ onAddRun }: { onAddRun: () => void }) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-6">
        <CalendarClock className="h-8 w-8 text-primary" />
      </div>
      <h3 className="text-xl font-semibold mb-2">No Scheduled Runs</h3>
      <p className="text-muted-foreground mb-6 max-w-md">
        Scheduled runs define how DQX automatically processes your data quality checks
        on a recurring or time-based cadence. Create your first schedule to get started.
      </p>
      <Button onClick={onAddRun} className="gap-2">
        <Plus className="h-4 w-4" />
        Create Your First Schedule
      </Button>
    </div>
  );
}

function RunNotFoundState({ runName, onAddRun }: { runName: string; onAddRun: () => void }) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-destructive/10 flex items-center justify-center mb-6">
        <AlertCircle className="h-8 w-8 text-destructive" />
      </div>
      <h3 className="text-xl font-semibold mb-2">Schedule Not Found</h3>
      <p className="text-muted-foreground mb-6 max-w-md">
        The schedule configuration{" "}
        <code className="px-1.5 py-0.5 bg-muted rounded text-sm font-mono">{runName}</code>{" "}
        does not exist. It may have been deleted or renamed.
      </p>
      <div className="flex gap-3">
        <Button variant="outline" asChild>
          <Link to="/runs">View All Schedules</Link>
        </Button>
        <Button onClick={onAddRun} className="gap-2">
          <Plus className="h-4 w-4" />
          Create New Schedule
        </Button>
      </div>
    </div>
  );
}

function SelectRunState() {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
        <CalendarClock className="h-8 w-8 text-muted-foreground" />
      </div>
      <h3 className="text-lg font-medium text-muted-foreground">Select a Schedule</h3>
      <p className="text-muted-foreground/70 text-sm mt-1">
        Choose a schedule from the list to view and edit its configuration
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
}: RunEditorProps) {
  const isLocked = isSaving || isDeleting || !!isRunning;
  const [editorMode, setEditorMode] = useState<"form" | "yaml">("form");

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between pb-4 border-b border-border/50">
        <div className="flex-1">
          <h2 className="text-2xl font-bold tracking-tight">{runName}</h2>
          <p className="text-muted-foreground text-sm mt-0.5">
            Edit schedule using {editorMode === "form" ? "form" : "YAML editor"}
            {isDirty && <span className="text-amber-500 ml-2">• Unsaved changes</span>}
          </p>
        </div>
        <div className="flex items-center gap-4">
          <Tabs value={editorMode} onValueChange={(v) => setEditorMode(v as "form" | "yaml")}>
            <TabsList>
              <TabsTrigger value="form" className="gap-2">
                <FormInput className="h-4 w-4" />
                Form
              </TabsTrigger>
              <TabsTrigger value="yaml" className="gap-2">
                <FileCode className="h-4 w-4" />
                YAML
              </TabsTrigger>
            </TabsList>
          </Tabs>
          <div className="h-8 w-px bg-border" />
          <div className="flex items-center gap-2">
            {onRunNow && (
              <Button variant="outline" size="sm" onClick={onRunNow} disabled={isDirty || isLocked} title={isDirty ? "Save changes before running" : "Run this schedule now"} className="gap-1.5">
                {isRunning ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                Run Now
              </Button>
            )}
            <Button variant="ghost" size="icon" onClick={onReset} disabled={!isDirty || isLocked} title="Reset changes">
              <RotateCcw className="h-4 w-4" />
            </Button>
            <Button onClick={onSave} variant="default" size="icon" disabled={!isDirty || isLocked} title="Save changes">
              {isSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            </Button>
            <AlertDialog open={isDeleteOpen} onOpenChange={setIsDeleteOpen}>
              <AlertDialogTrigger asChild>
                <Button variant="destructive" size="icon" disabled={isDeleting || isLocked} title="Delete Schedule">
                  <Trash2 className="h-4 w-4" />
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Delete Schedule</AlertDialogTitle>
                  <AlertDialogDescription>
                    Are you sure you want to delete the schedule{" "}
                    <span className="font-mono font-medium">{runName}</span>? This action cannot be undone.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel disabled={isDeleting}>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    onClick={(e) => { e.preventDefault(); if (!isDeleting) onDelete(); }}
                    disabled={isDeleting}
                    className="bg-destructive text-foreground hover:bg-destructive/90"
                  >
                    {isDeleting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
                    Delete
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        </div>
      </div>
      <div className="flex-1 min-h-0 mt-4">
        {editorMode === "form" ? (
          <FormEditor yamlContent={yamlContent} setYamlContent={setYamlContent} setIsDirty={setIsDirty} isLocked={isLocked} />
        ) : (
          <YamlEditor yamlContent={yamlContent} setYamlContent={setYamlContent} setIsDirty={setIsDirty} isLocked={isLocked} />
        )}
      </div>
    </div>
  );
}

function YamlEditor({
  yamlContent, setYamlContent, setIsDirty, isLocked,
}: { yamlContent: string; setYamlContent: (c: string) => void; setIsDirty: (d: boolean) => void; isLocked: boolean }) {
  return (
    <div className="h-full relative">
      <div className="absolute inset-0 rounded-lg border border-border/50 bg-muted/30 overflow-hidden">
        <textarea
          value={yamlContent}
          onChange={(e) => { setYamlContent(e.target.value); setIsDirty(true); }}
          disabled={isLocked}
          className={cn(
            "w-full h-full resize-none p-4",
            "font-mono text-sm leading-relaxed",
            "bg-transparent focus:outline-none",
            "placeholder:text-muted-foreground/50",
            isLocked && "opacity-50 cursor-not-allowed",
          )}
          spellCheck={false}
          placeholder="# Schedule configuration YAML..."
        />
      </div>
    </div>
  );
}

function FormEditor({
  yamlContent, setYamlContent, setIsDirty, isLocked,
}: { yamlContent: string; setYamlContent: (c: string) => void; setIsDirty: (d: boolean) => void; isLocked: boolean }) {
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
        if (parsed.sample_size != null) schedFromYaml.sample_size = parsed.sample_size;
        if (parsed.cron_expression) schedFromYaml.cron_expression = parsed.cron_expression;
        setScheduleConfig({ ...DEFAULT_SCHEDULE, ...schedFromYaml });
        setParseError(null);
      } else {
        setFormData(null);
      }
    } catch (e) {
      setParseError(e instanceof Error ? e.message : "Failed to parse YAML");
      setFormData(null);
    }
  }, [yamlContent]);

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
        toast.error("Failed to convert form data to YAML");
      }
      return updated;
    });
  }, [setYamlContent, setIsDirty]);

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
        toast.error("Failed to convert form data to YAML");
      }
      return updated;
    });
  }, [setYamlContent, setIsDirty]);

  if (parseError) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-destructive mx-auto mb-3" />
          <p className="text-destructive font-medium mb-1">Invalid YAML</p>
          <p className="text-muted-foreground text-sm">{parseError}</p>
          <p className="text-muted-foreground text-xs mt-2">Switch to YAML mode to fix the syntax</p>
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
          <h3 className="text-lg font-semibold mb-4">Basic Configuration</h3>
          <div className="space-y-4">
            <div className="grid gap-2">
              <Label htmlFor="name">Name</Label>
              <Input id="name" value={formData.name || ""} onChange={(e) => updateFormData({ name: e.target.value })} disabled={isLocked} placeholder="e.g., daily_sales_check" />
            </div>
          </div>
        </section>

        <section>
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <CalendarClock className="h-5 w-5" /> Schedule
          </h3>
          <p className="text-xs text-muted-foreground mb-3">
            Configure when this schedule should automatically run. Choose "Manual only" to disable automatic execution.
          </p>
          <ScheduleFrequencyPicker config={scheduleConfig} onChange={handleScheduleChange} disabled={isLocked} />
        </section>

        <section>
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Database className="h-5 w-5" /> Rule Scope
          </h3>
          <p className="text-xs text-muted-foreground mb-3">
            Choose which approved rule sets to include. Select by catalog, schema, or specific tables.
            Rules are read from the <code className="text-[10px] bg-muted px-1 py-0.5 rounded">dq_quality_rules</code> Delta table.
          </p>
          {approvedTables.length === 0 ? (
            <p className="text-sm text-muted-foreground">No approved rules found. Approve rules in Drafts & review first.</p>
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

