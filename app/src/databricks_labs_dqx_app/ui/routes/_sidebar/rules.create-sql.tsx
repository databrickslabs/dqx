import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState, useEffect, useMemo, useRef } from "react";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Database,
  Save,
  Loader2,
  ArrowLeft,
  AlertCircle,
  Plus,
  Trash2,
} from "lucide-react";
import { toast } from "sonner";
import { useSaveRules, useGetRules } from "@/lib/api";
import { checkDuplicates, type CheckDuplicatesIn } from "@/lib/api-custom";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Badge } from "@/components/ui/badge";

interface SearchParams {
  edit?: string;
}

export const Route = createFileRoute("/_sidebar/rules/create-sql")({
  component: CreateSqlCheckPage,
  validateSearch: (search: Record<string, unknown>): SearchParams => ({
    edit: typeof search.edit === "string" ? search.edit : undefined,
  }),
});

const SQL_CHECK_PREFIX = "__sql_check__/";

interface SqlCheckDraft {
  id: string;
  name: string;
  query: string;
  criticality: "warn" | "error";
  weight: number;
}

function newSqlCheck(): SqlCheckDraft {
  return {
    id: crypto.randomUUID(),
    name: "",
    query: "",
    criticality: "warn",
    weight: 3,
  };
}

function CreateSqlCheckPage() {
  const navigate = useNavigate();
  const { edit: editFqn } = Route.useSearch();
  const isEditMode = !!editFqn;

  const [checks, setChecks] = useState<SqlCheckDraft[]>([newSqlCheck()]);
  const [initialized, setInitialized] = useState(false);
  const saveMutation = useSaveRules();

  const { data: existingRule } = useGetRules(editFqn ?? "", {
    query: { enabled: !!editFqn },
  });

  useEffect(() => {
    if (!existingRule?.data || initialized) return;
    const entries = Array.isArray(existingRule.data) ? existingRule.data : [existingRule.data];
    const allChecks = entries.flatMap((e) => e.checks ?? []);
    const loaded: SqlCheckDraft[] = allChecks.map((c: Record<string, unknown>) => {
      const check = (c.check ?? {}) as Record<string, unknown>;
      const args = (check.arguments ?? {}) as Record<string, unknown>;
      return {
        id: crypto.randomUUID(),
        name: (c.name as string) ?? "",
        query: (args.query as string) ?? "",
        criticality: ((c.criticality as string) === "error" ? "error" : "warn") as "warn" | "error",
        weight: Number(c.weight ?? 3),
      };
    });
    if (loaded.length > 0) {
      setChecks(loaded);
    }
    setInitialized(true);
  }, [existingRule, initialized]);

  // ── Real-time duplicate detection ──────────────────────────────────
  const [dupCheckIds, setDupCheckIds] = useState<Set<string>>(new Set());
  const [dupChecking, setDupChecking] = useState(false);
  const dupTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const checksFingerprint = useMemo(() => {
    return checks.filter((c) => c.name.trim() && c.query.trim()).map((c) => `${c.id}:${c.name}:${c.query}`).join("|");
  }, [checks]);

  useEffect(() => {
    if (dupTimerRef.current) clearTimeout(dupTimerRef.current);
    const eligible = checks.filter((c) => c.name.trim() && c.query.trim());
    if (eligible.length === 0) { setDupCheckIds(new Set()); return; }

    dupTimerRef.current = setTimeout(async () => {
      setDupChecking(true);
      const dups = new Set<string>();
      for (const check of eligible) {
        const tableFqn = `${SQL_CHECK_PREFIX}${check.name.trim().replace(/\s+/g, "_").toLowerCase()}`;
        const payload = [{
          name: check.name.trim(),
          criticality: check.criticality,
          weight: check.weight,
          check: { function: "sql_query", arguments: { query: check.query.trim() } },
        }];
        try {
          const body: CheckDuplicatesIn = { table_fqn: tableFqn, checks: payload };
          const resp = await checkDuplicates(body);
          if (resp.data.duplicates.length > 0) dups.add(check.id);
        } catch {
          // ignore
        }
      }
      setDupCheckIds(dups);
      setDupChecking(false);
    }, 600);
    return () => { if (dupTimerRef.current) clearTimeout(dupTimerRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [checksFingerprint]);

  const hasDuplicates = dupCheckIds.size > 0;

  const addCheck = () => setChecks((prev) => [...prev, newSqlCheck()]);

  const removeCheck = (id: string) => {
    setChecks((prev) => (prev.length <= 1 ? prev : prev.filter((c) => c.id !== id)));
  };

  const updateCheck = (id: string, patch: Partial<SqlCheckDraft>) => {
    setChecks((prev) => prev.map((c) => (c.id === id ? { ...c, ...patch } : c)));
  };

  const isValid = checks.every((c) => c.name.trim() !== "" && c.query.trim() !== "");

  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    let successCount = 0;
    let failCount = 0;

    for (const check of checks) {
      const tableFqn = `${SQL_CHECK_PREFIX}${check.name.trim().replace(/\s+/g, "_").toLowerCase()}`;
      const checkPayload = [
        {
          name: check.name.trim(),
          criticality: check.criticality,
          weight: check.weight,
          check: {
            function: "sql_query",
            arguments: {
              query: check.query.trim(),
            },
          },
        },
      ];
      try {
        await saveMutation.mutateAsync({
          data: { table_fqn: tableFqn, checks: checkPayload },
        });
        successCount++;
      } catch {
        failCount++;
      }
    }

    setSaving(false);

    if (successCount > 0) {
      toast.success(`${successCount} SQL check${successCount > 1 ? "s" : ""} saved`);
    }
    if (failCount > 0) {
      toast.error(`${failCount} check${failCount > 1 ? "s" : ""} failed to save`);
    }
    if (successCount > 0 && failCount === 0) {
      navigate({ to: "/rules/drafts" });
    }
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb
          items={[{ label: "Create Rules", to: "/rules/create" }]}
          page={isEditMode ? "Edit SQL check" : "Cross-table rules"}
        />
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate({ to: "/rules/create" })}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <div>
            <h1 className="text-2xl font-bold tracking-tight">
              {isEditMode ? "Edit cross-table SQL check" : "Cross-table SQL checks"}
            </h1>
            <p className="text-muted-foreground">
              Write SQL queries that validate data across tables or run dataset-level aggregation checks.
            </p>
          </div>
        </div>
      </div>

      <div className="space-y-4">
        {checks.map((check, idx) => {
          const isDup = dupCheckIds.has(check.id);
          return (
          <Card key={check.id} className={isDup ? "border-red-300 bg-red-50/30" : ""}>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Database className="h-4 w-4" />
                  SQL check {checks.length > 1 ? `#${idx + 1}` : ""}
                  {isDup && (
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Badge variant="destructive" className="text-[10px] gap-1">
                            <AlertCircle className="h-2.5 w-2.5" />
                            Duplicate
                          </Badge>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>A SQL check with this name already exists. Remove or rename it.</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  )}
                </CardTitle>
                {checks.length > 1 && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 w-7 p-0 text-destructive"
                    onClick={() => removeCheck(check.id)}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </Button>
                )}
              </div>
              <CardDescription>
                The query should return rows that violate the check (i.e., bad rows).
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-4 sm:grid-cols-3">
                <div className="space-y-1.5">
                  <Label htmlFor={`name-${check.id}`}>Name</Label>
                  <Input
                    id={`name-${check.id}`}
                    placeholder="e.g. orders_total_matches_line_items"
                    value={check.name}
                    onChange={(e) => updateCheck(check.id, { name: e.target.value })}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label>Criticality</Label>
                  <Select
                    value={check.criticality}
                    onValueChange={(v) =>
                      updateCheck(check.id, { criticality: v as "warn" | "error" })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="warn">warn</SelectItem>
                      <SelectItem value="error">error</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1.5">
                  <Label>Weight (1–5)</Label>
                  <Select
                    value={String(check.weight)}
                    onValueChange={(v) => updateCheck(check.id, { weight: Number(v) })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="1">1 – Informational</SelectItem>
                      <SelectItem value="2">2 – Low</SelectItem>
                      <SelectItem value="3">3 – Medium</SelectItem>
                      <SelectItem value="4">4 – High</SelectItem>
                      <SelectItem value="5">5 – Critical</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="space-y-1.5">
                <Label htmlFor={`query-${check.id}`}>SQL query</Label>
                <Textarea
                  id={`query-${check.id}`}
                  className="font-mono text-sm min-h-[140px]"
                  placeholder={`SELECT o.order_id, o.total, SUM(li.amount) AS line_total\nFROM catalog.schema.orders o\nJOIN catalog.schema.line_items li ON li.order_id = o.order_id\nGROUP BY o.order_id, o.total\nHAVING o.total != line_total`}
                  value={check.query}
                  onChange={(e) => updateCheck(check.id, { query: e.target.value })}
                />
                <p className="text-xs text-muted-foreground">
                  Use fully qualified table names (catalog.schema.table). Returned rows are treated as violations.
                </p>
              </div>
            </CardContent>
          </Card>
          );
        })}

        <Button variant="outline" size="sm" onClick={addCheck} className="gap-1">
          <Plus className="h-3 w-3" />
          Add another SQL check
        </Button>
      </div>

      {/* Save bar */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 text-sm text-muted-foreground">
              {!isValid && (
                <>
                  <AlertCircle className="h-4 w-4 text-amber-500" />
                  Every check needs a name and a SQL query
                </>
              )}
              {isValid && hasDuplicates && (
                <>
                  <AlertCircle className="h-4 w-4 text-red-500" />
                  <span className="text-red-600">
                    {dupCheckIds.size} check{dupCheckIds.size !== 1 ? "s" : ""} already exist and cannot be saved
                  </span>
                </>
              )}
              {isValid && !hasDuplicates && (
                <span>
                  {checks.length} SQL check{checks.length > 1 ? "s" : ""} ready to save
                  {dupChecking && <Loader2 className="inline h-3 w-3 animate-spin ml-2" />}
                </span>
              )}
            </div>
            <Button
              onClick={handleSave}
              disabled={!isValid || saving || hasDuplicates}
              className="gap-2"
            >
              {saving ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Save className="h-4 w-4" />
              )}
              {saving ? "Saving..." : isEditMode ? "Save changes" : "Save as drafts"}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
