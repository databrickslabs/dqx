import { createFileRoute, Navigate, useNavigate } from "@tanstack/react-router";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  AlertTriangle,
  Camera,
  CheckCircle2,
  ExternalLink,
  Info,
  Loader2,
  Play,
  Save,
  Send,
  ShieldCheck,
} from "lucide-react";

import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
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
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { DryRunResults } from "@/components/DryRunResults";
import { LabelsEditor } from "@/components/Labels";
import { useJobPolling } from "@/hooks/use-job-polling";
import { filterDdlByColumns } from "@/lib/format-utils";
import {
  getTableSchemaDdl,
  saveRules,
  submitRuleForApproval,
  useGetDryRunResults,
  useGetRules,
  useSubmitDryRun,
  type DryRunResultsOut,
  type SaveRulesIn,
} from "@/lib/api";
import {
  cancelDryRun,
  getDryRunStatusCustom,
  useLabelDefinitions,
} from "@/lib/api-custom";

const DOCS_URL =
  "https://databrickslabs.github.io/dqx/docs/reference/api/check_funcs/#has_valid_schema";

const SQL_CHECK_PREFIX = "__sql_check__/";

// ``user_metadata`` keys this page owns and round-trips itself. Anything
// outside this set is treated as a free-form user label and routed through
// the LabelsEditor so it survives a save without losing the system fields.
const SCHEMA_SYSTEM_META_KEYS = new Set([
  "rule_type",
  "target_table",
  "strictness",
]);

type SchemaSourceMode = "ddl" | "ref_table";
type StrictMode = "strict" | "compatible";

interface SearchParams {
  /** Synthetic ``__sql_check__/<name>`` table_fqn to load in edit mode. */
  edit?: string;
  /** Origin page, used to wire "Cancel" back to a sensible place. */
  from?: string;
}

export const Route = createFileRoute("/_sidebar/rules/schema")({
  component: SchemaRulePage,
  validateSearch: (search: Record<string, unknown>): SearchParams => ({
    edit: typeof search.edit === "string" ? search.edit : undefined,
    from: typeof search.from === "string" ? search.from : undefined,
  }),
});

function SchemaRulePage() {
  // Thin guard so the inner component's hook count is stable across
  // permission-cache refreshes (see Rules of Hooks).
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;
  return <SchemaRulePageInner />;
}

function SchemaRulePageInner() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { edit: editFqn, from: fromPage } = Route.useSearch();
  const isEditMode = !!editFqn;
  const cancelTarget =
    fromPage === "active"
      ? "/rules/active"
      : fromPage === "drafts"
        ? "/rules/drafts"
        : "/rules/create";

  // Target table to apply the rule to.
  const [targetTable, setTargetTable] = useState("");
  // Rule name + criticality.
  const [ruleName, setRuleName] = useState("");
  const [criticality, setCriticality] = useState<"error" | "warn">("error");

  // Schema source.
  const [sourceMode, setSourceMode] = useState<SchemaSourceMode>("ddl");
  const [expectedDdl, setExpectedDdl] = useState("");
  const [refTable, setRefTable] = useState("");
  const [isSnapshotting, setIsSnapshotting] = useState(false);

  // Strict / compatible toggle.
  const [strict, setStrict] = useState<StrictMode>("compatible");

  // Subset / exclusion.
  const [columnsCsv, setColumnsCsv] = useState("");
  const [excludeColumnsCsv, setExcludeColumnsCsv] = useState("");

  // Free-form labels (everything in ``user_metadata`` that isn't a
  // schema-rule system key — see ``SCHEMA_SYSTEM_META_KEYS``).
  const [labels, setLabels] = useState<Record<string, string>>({});
  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = labelDefsData?.definitions ?? [];

  // Save state.
  const [isSaving, setIsSaving] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // When set, save becomes an update of an existing catalog entry.
  // Populated by the edit-mode loader below.
  const [editingRuleId, setEditingRuleId] = useState<string | undefined>(
    undefined,
  );
  const [editLoaded, setEditLoaded] = useState(false);
  const { data: editRuleResp } = useGetRules(editFqn ?? "", {
    query: { enabled: isEditMode },
  });

  // Hydrate the form when the catalog entry loads. The page always saves a
  // single ``has_valid_schema`` rule per synthetic ``__sql_check__/<name>``
  // table_fqn, so we look at the first matching check and ignore anything
  // exotic — better to leave the form pristine than to silently merge a
  // foreign rule's args into the editor.
  useEffect(() => {
    if (!isEditMode || editLoaded || !editRuleResp?.data) return;
    const entries = Array.isArray(editRuleResp.data)
      ? editRuleResp.data
      : [editRuleResp.data];
    const entry = entries.find((e) =>
      (e.checks ?? []).some(
        (c) =>
          ((c as Record<string, unknown>).check as Record<string, unknown>)
            ?.function === "has_valid_schema",
      ),
    );
    if (!entry) {
      toast.error(t("rulesSchema.edit.notSchemaRule"));
      setEditLoaded(true);
      return;
    }
    const check = (entry.checks ?? []).find(
      (c) =>
        ((c as Record<string, unknown>).check as Record<string, unknown>)
          ?.function === "has_valid_schema",
    ) as Record<string, unknown> | undefined;
    if (!check) {
      setEditLoaded(true);
      return;
    }
    const checkBody = (check.check ?? {}) as Record<string, unknown>;
    const args = (checkBody.arguments ?? {}) as Record<string, unknown>;
    const userMeta = (check.user_metadata ?? {}) as Record<string, unknown>;

    setRuleName(String(check.name ?? ""));
    setCriticality(
      ((check.criticality as string) === "warn" ? "warn" : "error") as
        | "error"
        | "warn",
    );
    setTargetTable(
      typeof userMeta.target_table === "string" ? userMeta.target_table : "",
    );

    if (typeof args.ref_table === "string" && args.ref_table) {
      setSourceMode("ref_table");
      setRefTable(args.ref_table);
      setExpectedDdl("");
    } else {
      setSourceMode("ddl");
      setExpectedDdl(
        typeof args.expected_schema === "string" ? args.expected_schema : "",
      );
      setRefTable("");
    }

    // ``strict`` is canonically a bool in args, but older / hand-edited
    // rules sometimes stash the human-readable strictness in user_metadata.
    // Trust the args value first, then fall back to the metadata token.
    if (typeof args.strict === "boolean") {
      setStrict(args.strict ? "strict" : "compatible");
    } else if (userMeta.strictness === "strict") {
      setStrict("strict");
    } else {
      setStrict("compatible");
    }

    const colsArg = Array.isArray(args.columns) ? args.columns : [];
    setColumnsCsv(colsArg.filter((c) => typeof c === "string").join(", "));
    const exclArg = Array.isArray(args.exclude_columns)
      ? args.exclude_columns
      : [];
    setExcludeColumnsCsv(
      exclArg.filter((c) => typeof c === "string").join(", "),
    );

    // Split user_metadata into system fields (round-tripped by this page)
    // and free-form labels (rendered through LabelsEditor). Without this
    // split the editor would either drop user labels on save or let users
    // overwrite the system fields that drive the catalog grouping.
    const freeLabels: Record<string, string> = {};
    for (const [k, v] of Object.entries(userMeta)) {
      if (SCHEMA_SYSTEM_META_KEYS.has(k)) continue;
      if (typeof v === "string") freeLabels[k] = v;
      else if (typeof v === "number" || typeof v === "boolean") {
        freeLabels[k] = String(v);
      }
    }
    setLabels(freeLabels);

    setEditingRuleId(
      typeof entry.rule_id === "string" && entry.rule_id.length > 0
        ? entry.rule_id
        : undefined,
    );
    setEditLoaded(true);
  }, [isEditMode, editLoaded, editRuleResp, t]);

  // Dry-run state. Mirrors the single-table page so it slots cleanly
  // into the existing job-polling + cleanup pipeline:
  //   * ``submitDryRun`` kicks off a Databricks job
  //   * ``useJobPolling`` polls until terminal
  //   * ``getDryRunResults`` fetches the materialised results on success
  // For schema validation the dry-run table IS the rule's target table —
  // there's no per-rule fan-out to disambiguate, unlike single-table
  // rules which can have many targets per check.
  const [dryRunSampleSize, setDryRunSampleSize] = useState(1000);
  const [dryRunResult, setDryRunResult] = useState<DryRunResultsOut | null>(
    null,
  );
  const [dryRunRunId, setDryRunRunId] = useState<string | null>(null);
  const [dryRunJobRunId, setDryRunJobRunId] = useState<number | null>(null);
  const [dryRunViewFqn, setDryRunViewFqn] = useState<string | null>(null);

  const submitDryRunMutation = useSubmitDryRun();
  const dryRunResultsQuery = useGetDryRunResults(dryRunRunId ?? "", {
    query: { enabled: false },
  });

  const fetchDryRunStatus = useCallback(async () => {
    if (!dryRunRunId || dryRunJobRunId === null) {
      throw new Error("No active dry run");
    }
    const resp = await getDryRunStatusCustom(dryRunRunId, {
      job_run_id: dryRunJobRunId,
      view_fqn: dryRunViewFqn ?? undefined,
    });
    return resp.data;
  }, [dryRunRunId, dryRunJobRunId, dryRunViewFqn]);

  const dryRunPolling = useJobPolling({
    fetchStatus: fetchDryRunStatus,
    enabled: dryRunJobRunId !== null && dryRunRunId !== null,
    interval: 3000,
    onComplete: async (status) => {
      if (status.result_state === "SUCCESS") {
        try {
          const resp = await dryRunResultsQuery.refetch();
          if (resp.data?.data) {
            setDryRunResult(resp.data.data);
            toast.success(t("rulesSchema.dryRun.toastComplete"));
          }
        } catch {
          toast.error(t("rulesSchema.dryRun.toastFetchFailed"));
        }
      } else {
        toast.error(
          t("rulesSchema.dryRun.toastFailed", {
            message:
              status.message || t("rulesSchema.dryRun.toastUnknownError"),
          }),
        );
      }
      setDryRunJobRunId(null);
    },
    onError: () => {
      toast.error(t("rulesSchema.dryRun.toastStatusFailed"));
    },
  });

  const handleSnapshot = async (fqn: string) => {
    if (!fqn.trim()) {
      toast.error(t("rulesSchema.errors.pickTable"));
      return;
    }
    setIsSnapshotting(true);
    try {
      const resp = await getTableSchemaDdl({ table_fqn: fqn });
      const ddl = resp.data.ddl;
      if (!ddl) {
        toast.error(t("rulesSchema.errors.emptyDdl"));
      } else {
        setExpectedDdl(ddl);
        toast.success(
          t("rulesSchema.snapshotDone", { count: resp.data.column_count ?? 0 }),
        );
      }
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail ?? t("rulesSchema.errors.snapshotFailed");
      toast.error(detail);
    } finally {
      setIsSnapshotting(false);
    }
  };

  const parsedColumns = useMemo(
    () => splitCsv(columnsCsv),
    [columnsCsv],
  );
  const parsedExcludeColumns = useMemo(
    () => splitCsv(excludeColumnsCsv),
    [excludeColumnsCsv],
  );

  const validation = useMemo(
    () =>
      validate({
        targetTable,
        ruleName,
        sourceMode,
        expectedDdl,
        refTable,
      }),
    [targetTable, ruleName, sourceMode, expectedDdl, refTable],
  );

  // ``has_valid_schema`` is dataset-level. We mirror the existing
  // cross-table-SQL convention so saving / listing already knows how
  // to bucket dataset rules: the catalog row uses an
  // ``__sql_check__/<name>`` table_fqn so it survives a YAML round-trip,
  // while the rule's filter/check arguments still target the real
  // ``targetTable``.
  const buildRule = useCallback((): Record<string, unknown> => {
    const args: Record<string, unknown> = { strict: strict === "strict" };
    if (sourceMode === "ddl") {
      // DQX's ``has_valid_schema`` ``columns`` arg only filters the
      // *actual* dataframe — the expected schema is compared in full.
      // Without trimming the DDL we set "validate only email_address"
      // against a 10-column expected schema would (incorrectly) report
      // every other expected column as "missing in checked data".
      // Mirror the same subset on the expected side so both halves of
      // the comparison are aligned. We still pass ``columns`` /
      // ``exclude_columns`` so strict-mode field-order checks work.
      let ddl = expectedDdl.trim();
      if (parsedColumns.length > 0) {
        ddl = filterDdlByColumns(ddl, parsedColumns, "include");
      }
      if (parsedExcludeColumns.length > 0) {
        ddl = filterDdlByColumns(ddl, parsedExcludeColumns, "exclude");
      }
      args.expected_schema = ddl;
    } else {
      // ref_table mode: we can't trim a remote schema client-side, so
      // we surface a UI warning instead and pass the subset args
      // through to DQX as-is.
      args.ref_table = refTable.trim();
    }
    if (parsedColumns.length > 0) args.columns = parsedColumns;
    if (parsedExcludeColumns.length > 0) {
      args.exclude_columns = parsedExcludeColumns;
    }
    // System keys (rule_type, target_table, strictness) always win over
    // any user-supplied label of the same name — otherwise a user could
    // accidentally repoint the rule by adding a ``target_table`` label.
    const userMetadata: Record<string, string> = {};
    for (const [k, v] of Object.entries(labels)) {
      if (SCHEMA_SYSTEM_META_KEYS.has(k)) continue;
      userMetadata[k] = v;
    }
    userMetadata.rule_type = "schema_validation";
    // The actual table this validates lives in metadata too — the
    // ``table_fqn`` slot is a synthetic key only.
    userMetadata.target_table = targetTable.trim();
    userMetadata.strictness = strict;
    return {
      name: ruleName.trim(),
      criticality,
      check: {
        function: "has_valid_schema",
        arguments: args,
      },
      user_metadata: userMetadata,
    };
  }, [
    strict,
    sourceMode,
    expectedDdl,
    refTable,
    parsedColumns,
    parsedExcludeColumns,
    ruleName,
    criticality,
    targetTable,
    labels,
  ]);

  // Fingerprint of every input that affects the rule body. Whenever the
  // user edits anything that would change what the dry run measures, we
  // wipe stale results so the UI never claims success against a
  // different set of inputs than the one currently on screen.
  const ruleFingerprint = useMemo(
    () =>
      [
        targetTable,
        ruleName,
        criticality,
        sourceMode,
        expectedDdl,
        refTable,
        strict,
        parsedColumns.join(","),
        parsedExcludeColumns.join(","),
      ].join("|"),
    [
      targetTable,
      ruleName,
      criticality,
      sourceMode,
      expectedDdl,
      refTable,
      strict,
      parsedColumns,
      parsedExcludeColumns,
    ],
  );
  const lastRunFingerprintRef = useRef<string | null>(null);
  useEffect(() => {
    if (
      dryRunResult &&
      lastRunFingerprintRef.current !== null &&
      lastRunFingerprintRef.current !== ruleFingerprint
    ) {
      setDryRunResult(null);
    }
  }, [ruleFingerprint, dryRunResult]);

  const isDryRunning =
    submitDryRunMutation.isPending || dryRunPolling.isPolling;

  // Best-effort cancel if the user navigates away mid-run. We rely on
  // the unmount path rather than route guards because schema rules
  // don't have unsaved drafts to warn about — the rule isn't persisted
  // until the user explicitly clicks save/submit.
  useEffect(() => {
    return () => {
      if (dryRunRunId && dryRunJobRunId !== null) {
        cancelDryRun(dryRunRunId, { job_run_id: dryRunJobRunId }).catch(() => {
          // ignore — best effort
        });
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleDryRun = async () => {
    if (!validation.valid) {
      toast.error(validation.message ?? t("rulesSchema.errors.invalidForm"));
      return;
    }
    try {
      setDryRunResult(null);
      const rule = buildRule();
      const resp = await submitDryRunMutation.mutateAsync({
        data: {
          table_fqn: targetTable.trim(),
          checks: [rule],
          sample_size: dryRunSampleSize,
          skip_history: true,
        },
      });
      setDryRunRunId(resp.data.run_id);
      setDryRunJobRunId(resp.data.job_run_id);
      setDryRunViewFqn(resp.data.view_fqn ?? null);
      lastRunFingerprintRef.current = ruleFingerprint;
      toast.info(t("rulesSchema.dryRun.toastSubmitted"));
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail;
      toast.error(
        detail
          ? t("rulesSchema.dryRun.toastSubmitFailedDetail", { detail })
          : t("rulesSchema.dryRun.toastSubmitFailed"),
      );
    }
  };

  const persistAs = async (alsoSubmit: boolean) => {
    if (!validation.valid) {
      toast.error(validation.message ?? t("rulesSchema.errors.invalidForm"));
      return;
    }
    if (alsoSubmit) setIsSubmitting(true);
    else setIsSaving(true);
    try {
      const rule = buildRule();
      // When editing, route the save through ``rule_id`` so the backend
      // upserts in place. The synthetic ``__sql_check__/<name>`` table_fqn
      // is recomputed from the (possibly renamed) rule name so a name
      // change still moves the catalog row, while ``rule_id`` ensures the
      // identity persists across the move.
      if (editingRuleId) {
        rule.rule_id = editingRuleId;
      }
      // Reuse the cross-table SQL bucket so the rule shows up in the
      // dataset-level rules view, not in the per-column listing.
      const tableFqn = `${SQL_CHECK_PREFIX}${ruleName.trim()}`;
      const payload: SaveRulesIn = {
        table_fqn: tableFqn,
        checks: [rule],
        source: "ui",
      };
      const resp = await saveRules(payload);

      if (alsoSubmit) {
        let submitted = 0;
        let failed = 0;
        for (const r of resp.data) {
          if (!r.rule_id) continue;
          try {
            await submitRuleForApproval(r.rule_id, null);
            submitted++;
          } catch {
            failed++;
          }
        }
        if (submitted > 0) {
          toast.success(t("rulesSchema.submitted", { count: submitted }));
        }
        if (failed > 0) {
          toast.error(t("rulesSchema.submitFailed", { count: failed }));
        }
      } else {
        toast.success(
          editingRuleId
            ? t("rulesSchema.savedUpdate")
            : t("rulesSchema.savedDraft"),
        );
      }
      // After editing, send users back where they came from so the list
      // they were browsing reflects the change immediately. New rules
      // always land in the drafts queue.
      navigate({ to: isEditMode ? cancelTarget : "/rules/drafts" });
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail ?? t("rulesSchema.errors.saveFailed");
      toast.error(detail);
    } finally {
      setIsSaving(false);
      setIsSubmitting(false);
    }
  };

  const busy = isSaving || isSubmitting || isDryRunning;

  return (
    <div className="space-y-6">
      <PageBreadcrumb
        items={[{ label: t("rulesCreate.breadcrumb"), to: "/rules/create" }]}
        page={
          isEditMode
            ? t("rulesSchema.edit.breadcrumb")
            : t("rulesSchema.breadcrumb")
        }
      />
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            {isEditMode ? t("rulesSchema.edit.title") : t("rulesSchema.title")}
          </h1>
          <p className="text-muted-foreground">
            {isEditMode
              ? t("rulesSchema.edit.subtitle")
              : t("rulesSchema.subtitle")}
          </p>
        </div>
        <a
          href={DOCS_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1.5 mt-1"
        >
          {t("rulesSchema.viewDocs")}
          <ExternalLink className="h-3 w-3" />
        </a>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <ShieldCheck className="h-4 w-4" />
            {t("rulesSchema.basics.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesSchema.basics.description")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label>{t("rulesSchema.basics.targetTable")}</Label>
              <CatalogBrowser
                value={targetTable}
                onChange={setTargetTable}
              />
              <p className="text-[11px] text-muted-foreground">
                {t("rulesSchema.basics.targetTableHint")}
              </p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="rule-name">{t("rulesSchema.basics.name")}</Label>
              <Input
                id="rule-name"
                value={ruleName}
                onChange={(e) => setRuleName(e.target.value)}
                placeholder="orders_schema_must_match"
                disabled={busy}
              />
              <p className="text-[11px] text-muted-foreground">
                {t("rulesSchema.basics.nameHint")}
              </p>
            </div>
          </div>
          <div className="flex items-start gap-3 p-3 rounded-lg border max-w-md">
            <div className="flex-1 space-y-1">
              <div className="text-sm font-medium">
                {t("rulesSchema.basics.criticality")}
              </div>
              <div className="text-xs text-muted-foreground">
                {t("rulesSchema.basics.criticalityHint")}
              </div>
            </div>
            <Select
              value={criticality}
              onValueChange={(v) => setCriticality(v as "error" | "warn")}
              disabled={busy}
            >
              <SelectTrigger className="w-[110px] h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="error">error</SelectItem>
                <SelectItem value="warn">warn</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {t("rulesSchema.source.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesSchema.source.description")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <ModeCard
              active={sourceMode === "ddl"}
              onClick={() => setSourceMode("ddl")}
              title={t("rulesSchema.source.ddl.title")}
              description={t("rulesSchema.source.ddl.description")}
              disabled={busy}
            />
            <ModeCard
              active={sourceMode === "ref_table"}
              onClick={() => setSourceMode("ref_table")}
              title={t("rulesSchema.source.ref.title")}
              description={t("rulesSchema.source.ref.description")}
              disabled={busy}
            />
          </div>

          {sourceMode === "ddl" ? (
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2 flex-wrap">
                <Label>{t("rulesSchema.source.ddl.label")}</Label>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleSnapshot(targetTable)}
                    disabled={!targetTable || isSnapshotting || busy}
                    className="gap-2"
                  >
                    {isSnapshotting ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Camera className="h-4 w-4" />
                    )}
                    {t("rulesSchema.source.ddl.snapshotFromTarget")}
                  </Button>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Info className="h-3.5 w-3.5 text-muted-foreground cursor-help" />
                    </TooltipTrigger>
                    <TooltipContent className="max-w-sm">
                      <p className="text-xs leading-relaxed">
                        {t("rulesSchema.source.ddl.snapshotHint")}
                      </p>
                    </TooltipContent>
                  </Tooltip>
                </div>
              </div>
              <Textarea
                value={expectedDdl}
                onChange={(e) => setExpectedDdl(e.target.value)}
                placeholder="id INT, name STRING, amount DECIMAL(10,2)"
                className="font-mono text-xs min-h-[140px]"
                disabled={busy}
              />
              <p className="text-[11px] text-muted-foreground">
                {t("rulesSchema.source.ddl.hint")}
              </p>
            </div>
          ) : (
            <div className="space-y-2">
              <Label>{t("rulesSchema.source.ref.label")}</Label>
              <CatalogBrowser value={refTable} onChange={setRefTable} />
              <p className="text-[11px] text-muted-foreground">
                {t("rulesSchema.source.ref.hint")}
              </p>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {t("rulesSchema.strictness.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesSchema.strictness.description")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <StrictnessOption
            active={strict === "compatible"}
            onClick={() => setStrict("compatible")}
            title={t("rulesSchema.strictness.compatible.title")}
            tagline={t("rulesSchema.strictness.compatible.tagline")}
            bullets={[
              t("rulesSchema.strictness.compatible.bullet1"),
              t("rulesSchema.strictness.compatible.bullet2"),
              t("rulesSchema.strictness.compatible.bullet3"),
            ]}
            disabled={busy}
            badge={t("rulesSchema.strictness.recommended")}
          />
          <StrictnessOption
            active={strict === "strict"}
            onClick={() => setStrict("strict")}
            title={t("rulesSchema.strictness.strict.title")}
            tagline={t("rulesSchema.strictness.strict.tagline")}
            bullets={[
              t("rulesSchema.strictness.strict.bullet1"),
              t("rulesSchema.strictness.strict.bullet2"),
              t("rulesSchema.strictness.strict.bullet3"),
            ]}
            disabled={busy}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {t("rulesSchema.advanced.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesSchema.advanced.description")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="cols">{t("rulesSchema.advanced.columns")}</Label>
              <Input
                id="cols"
                value={columnsCsv}
                onChange={(e) => setColumnsCsv(e.target.value)}
                placeholder="id, name, amount"
                disabled={busy}
              />
              <p className="text-[11px] text-muted-foreground">
                {t("rulesSchema.advanced.columnsHint")}
              </p>
              {parsedColumns.length > 0 && (
                <ColumnPills items={parsedColumns} />
              )}
            </div>
            <div className="space-y-2">
              <Label htmlFor="excl">{t("rulesSchema.advanced.exclude")}</Label>
              <Input
                id="excl"
                value={excludeColumnsCsv}
                onChange={(e) => setExcludeColumnsCsv(e.target.value)}
                placeholder="ingested_at, _rescued_data"
                disabled={busy}
              />
              <p className="text-[11px] text-muted-foreground">
                {t("rulesSchema.advanced.excludeHint")}
              </p>
              {parsedExcludeColumns.length > 0 && (
                <ColumnPills items={parsedExcludeColumns} />
              )}
            </div>
          </div>

          {/* ref_table mode caveat — we can't trim a remote schema on the
              client, so the user gets DQX's raw behaviour (``columns``
              only filters the actual side). Surface this explicitly so
              "the rule fires every time even though I subset" doesn't
              look like a bug. */}
          {sourceMode === "ref_table" &&
            (parsedColumns.length > 0 || parsedExcludeColumns.length > 0) && (
              <div className="flex items-start gap-2 p-3 rounded-lg border border-amber-500/30 bg-amber-50 dark:bg-amber-950/30 text-amber-700 dark:text-amber-400 text-sm">
                <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                <div className="space-y-1 min-w-0">
                  <div className="text-xs font-medium">
                    {t("rulesSchema.advanced.refSubsetWarningTitle")}
                  </div>
                  <div className="text-[11px] opacity-90">
                    {t("rulesSchema.advanced.refSubsetWarningBody")}
                  </div>
                </div>
              </div>
            )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {t("rulesSchema.labels.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesSchema.labels.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <LabelsEditor
            value={labels}
            onChange={setLabels}
            disabled={busy}
            defaultOpen={Object.keys(labels).length > 0}
            definitions={labelDefinitions}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <Play className="h-4 w-4" />
            {t("rulesSchema.dryRun.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesSchema.dryRun.description")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex items-center gap-1.5">
              <Label className="text-xs text-muted-foreground whitespace-nowrap flex items-center gap-1">
                {t("rulesSchema.dryRun.sampleRows")}
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Info className="h-3 w-3 cursor-help" />
                  </TooltipTrigger>
                  <TooltipContent className="max-w-xs">
                    <p className="text-xs leading-relaxed">
                      {t("rulesSchema.dryRun.sampleRowsHint")}
                    </p>
                  </TooltipContent>
                </Tooltip>
              </Label>
              <Input
                type="number"
                min={1}
                max={10000}
                value={dryRunSampleSize}
                onChange={(e) =>
                  setDryRunSampleSize(
                    Math.min(
                      10000,
                      Math.max(1, Number(e.target.value) || 1000),
                    ),
                  )
                }
                disabled={busy}
                className="w-24 h-9 text-xs"
              />
              <span className="text-[10px] text-muted-foreground whitespace-nowrap">
                {t("rulesSchema.dryRun.maxRows")}
              </span>
            </div>
            <Button
              variant="outline"
              onClick={handleDryRun}
              disabled={!validation.valid || busy}
              className="gap-2"
              size="sm"
            >
              {isDryRunning ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              {isDryRunning
                ? t("rulesSchema.dryRun.running")
                : t("rulesSchema.dryRun.run")}
            </Button>
            {!validation.valid && (
              <span className="text-[11px] text-muted-foreground">
                {t("rulesSchema.dryRun.needsValidRule")}
              </span>
            )}
          </div>

          {isDryRunning && dryRunPolling.status && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span>
                {t("rulesSchema.dryRun.jobStatus")}{" "}
                <span className="font-medium">
                  {dryRunPolling.status.state}
                </span>
              </span>
            </div>
          )}

          {dryRunResult && (
            <>
              <Separator />
              <DryRunResults result={dryRunResult} />
            </>
          )}
        </CardContent>
      </Card>

      <Separator />

      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="text-xs text-muted-foreground flex items-center gap-2">
          {validation.valid ? (
            <>
              <CheckCircle2 className="h-4 w-4 text-green-600" />
              {t("rulesSchema.readyToSave")}
            </>
          ) : (
            <>
              <AlertTriangle className="h-4 w-4 text-amber-600" />
              {validation.message ?? t("rulesSchema.errors.invalidForm")}
            </>
          )}
        </div>
        <div className="flex items-center gap-2">
          {isEditMode && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => navigate({ to: cancelTarget })}
              disabled={busy}
            >
              {t("common.cancel")}
            </Button>
          )}
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => persistAs(false)}
            disabled={!validation.valid || busy}
          >
            {isSaving ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Save className="h-4 w-4" />
            )}
            {isEditMode
              ? t("rulesSchema.updateRule")
              : t("rulesSchema.saveAsDraft")}
          </Button>
          <Button
            size="sm"
            className="gap-2"
            onClick={() => persistAs(true)}
            disabled={!validation.valid || busy}
          >
            {isSubmitting ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
            {t("rulesSchema.submitForReview")}
          </Button>
        </div>
      </div>
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

function splitCsv(value: string): string[] {
  return value
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
}

interface ValidationResult {
  valid: boolean;
  message?: string;
}

function validate({
  targetTable,
  ruleName,
  sourceMode,
  expectedDdl,
  refTable,
}: {
  targetTable: string;
  ruleName: string;
  sourceMode: SchemaSourceMode;
  expectedDdl: string;
  refTable: string;
}): ValidationResult {
  const t = targetTable.trim();
  if (!t || t.split(".").length !== 3) {
    return { valid: false, message: "Pick a fully qualified target table." };
  }
  const name = ruleName.trim();
  if (!name) return { valid: false, message: "Give your rule a name." };
  if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
    return {
      valid: false,
      message:
        "Rule names must start with a letter and contain only letters, digits, and underscores.",
    };
  }
  if (sourceMode === "ddl") {
    if (!expectedDdl.trim()) {
      return { valid: false, message: "Provide an expected DDL schema." };
    }
  } else {
    const r = refTable.trim();
    if (!r || r.split(".").length !== 3) {
      return {
        valid: false,
        message: "Pick a fully qualified reference table.",
      };
    }
  }
  return { valid: true };
}

function ModeCard({
  active,
  onClick,
  title,
  description,
  disabled,
}: {
  active: boolean;
  onClick: () => void;
  title: string;
  description: string;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={
        "text-left p-3 rounded-lg border transition-colors " +
        (active
          ? "border-primary bg-primary/5"
          : "hover:bg-muted/30 border-border") +
        (disabled ? " opacity-60 cursor-not-allowed" : "")
      }
    >
      <div className="text-sm font-medium">{title}</div>
      <div className="text-xs text-muted-foreground mt-1">{description}</div>
    </button>
  );
}

function StrictnessOption({
  active,
  onClick,
  title,
  tagline,
  bullets,
  disabled,
  badge,
}: {
  active: boolean;
  onClick: () => void;
  title: string;
  tagline: string;
  bullets: string[];
  disabled?: boolean;
  badge?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={
        "w-full text-left p-3 rounded-lg border transition-colors " +
        (active
          ? "border-primary bg-primary/5"
          : "hover:bg-muted/30 border-border") +
        (disabled ? " opacity-60 cursor-not-allowed" : "")
      }
    >
      <div className="flex items-center gap-2">
        <div className="text-sm font-medium">{title}</div>
        {badge && (
          <Badge variant="secondary" className="text-[10px]">
            {badge}
          </Badge>
        )}
      </div>
      <div className="text-xs text-muted-foreground mt-0.5">{tagline}</div>
      <ul className="mt-2 space-y-0.5 text-[11px] text-muted-foreground list-disc pl-4">
        {bullets.map((b, i) => (
          <li key={i}>{b}</li>
        ))}
      </ul>
    </button>
  );
}

function ColumnPills({ items }: { items: string[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {items.map((c) => (
        <Badge key={c} variant="outline" className="text-[10px] font-mono">
          {c}
        </Badge>
      ))}
    </div>
  );
}
