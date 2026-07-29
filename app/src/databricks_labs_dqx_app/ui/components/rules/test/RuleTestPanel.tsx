// RuleTestPanel — the Rules Registry "Test" tab surface (P22-E). Ports
// dqlake's `test/TestRuleTab.tsx` in spirit, adapted to DQX's registry rule
// model: a rule is tested by evaluating its effective SQL predicate over manual
// rows (inline VALUES) or a real UC table sample on the configured warehouse
// rows (OBO). `sql` / `lowcode` rules send a predicate; `dqx_native` rules send
// `function` + `native_arguments` which the backend compiles to SQL.
//
// Subtle behaviours carried over from dqlake:
//  - Manual / Table mode toggle; switching resets the last result.
//  - Manual grid: typed cells per slot family, add/edit/delete rows, Enter to
//    add+focus a new row, green/red row tinting after a run.
//  - "Generate test data" (AI) fills a passing/failing mix; gated + degrades
//    when AI is off.
//
// Beyond dqlake, which only ever tested a single table's rows:
//  - A cross-table rule gets one manual grid PER data source — the table being
//    checked plus one per table its query joins (read off the query's own FROM /
//    JOIN clauses), whose columns the author owns (they mirror a real table, not
//    the rule's slots) and which are seeded from the rule's own join conditions.
//    So an orphan check can be exercised on fabricated rows without creating a
//    single table.
//  - Such a rule runs as a whole query, which decides for itself which rows come
//    back (it may aggregate to one), so its verdicts can't be tinted onto input
//    rows — the query's output is shown as its own grid instead.
//  - Editing any manual row invalidates the previous verdicts (result cleared).
//  - Manual rows survive tab switches (module cache), and rebuild fresh when
//    the rule's logic (predicate/polarity/slots) changes.
//  - Warehouse pre-warm on mount; Run is disabled with a "Waiting for
//    Warehouse" tooltip until the warehouse is ready.
//  - Table mode: pick table + map slots, random-sample / full selector, tinted
//    read-only result grid, empty + truncation states.

import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Loader2, Sparkles } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { AI_GRADIENT_URL } from "@/lib/ai-style";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";
import { sqlEditorShape } from "@/lib/lowcodeCompile";
import {
  useRunRuleTest,
  useGenerateRuleTestData,
  type RuleSlot,
  type RuleTestRunOut,
} from "@/lib/api";
import { findReferenceTables, inferRefTableColumns } from "@/lib/refTableColumns";
import { ManualGrid, type ManualColumn } from "./ManualGrid";
import { ResultGrid, type ResultColumn } from "./ResultGrid";
import { TableTestSource, type TableSourcePayload } from "./TableTestSource";
import type { CellFamily } from "./TypedCell";
import { useWarehousePrewarm } from "./useWarehousePrewarm";
import { getManual, setManual, type ManualState, type RefGridState } from "./manualCache";

type Mode = "adhoc" | "table";
type SampleKind = "records" | "percent" | "full";

/** Forces a "Waiting for Warehouse" tooltip open while `show` is true. The
 *  wrapped button is disabled meanwhile, so it emits no hover events — hence
 *  the controlled-open tooltip (mirrors dqlake). */
function WarehouseWaitTooltip({ show, label, children }: { show: boolean; label: string; children: React.ReactNode }) {
  if (!show) return <>{children}</>;
  return (
    <TooltipProvider>
      <Tooltip open>
        <TooltipTrigger asChild>
          <span className="inline-flex">{children}</span>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

function DisabledTooltip({
  disabled,
  label,
  children,
  className,
}: {
  disabled: boolean;
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  if (!disabled) return <>{children}</>;
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={cn("inline-flex", className)}>{children}</span>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** First unused `column_N` for a reference grid the author is filling by hand. */
function nextRefColumnName(existing: string[]): string {
  const taken = new Set(existing);
  for (let i = 1; ; i++) if (!taken.has(`column_${i}`)) return `column_${i}`;
}

/** Seed the manual grids: one for the table being checked (columns = the rule's
 *  slots) plus one per table the rule joins, pre-headed with the columns the SQL
 *  reads off it (see `inferRefTableColumns`) so the author isn't handed a blank
 *  canvas. */
function buildInitialManual(slots: RuleSlot[], predicate: string, refTables: string[]): ManualState {
  const columns = slots.length ? slots.map((s) => s.name) : ["value"];
  const refs: Record<string, RefGridState> = {};
  for (const table of refTables) {
    const inferred = inferRefTableColumns(predicate, table);
    refs[table] = {
      columns: inferred,
      rows: inferred.length ? [inferred.map(() => null)] : [],
      families: Object.fromEntries(inferred.map((c) => [c, "any"])),
    };
  }
  return { columns, rows: [columns.map(() => null)], refs };
}

const CACHE_KEY = "registry-rule-test";

export function RuleTestPanel({
  predicate,
  polarity,
  slots,
  canTest,
  ruleMode = "sql",
  lowcodeAdvanced = false,
  nativeFunction,
  nativeArguments,
}: {
  /** Effective SQL predicate: raw in SQL mode, compiled AST in Low-Code mode. Unused for dqx_native. */
  predicate: string;
  polarity: "pass" | "fail";
  slots: RuleSlot[];
  /** Whether there is enough definition to test against. */
  canTest: boolean;
  /** Rule authoring mode — forwarded to the run route so it records the real mode. */
  ruleMode?: "sql" | "lowcode" | "dqx_native";
  /** True when a Low-Code rule folds joins/group-by into a dataset-level query;
   *  forwarded so the route can reject a mis-testable rule (belt-and-braces —
   *  the parent already hides the surface for these). */
  lowcodeAdvanced?: boolean;
  /** DQX Native check function name (required when ruleMode is dqx_native). */
  nativeFunction?: string;
  /** Frozen native body.arguments with {{slot}} placeholders. */
  nativeArguments?: Record<string, unknown>;
}) {
  const { t } = useTranslation();
  const ai = useAiAvailability();
  const { ready: warehouseReady } = useWarehousePrewarm();

  // Whole-SELECT rules (`sql_query`) run as a query against the data instead of
  // as a per-row predicate, which changes what the result grid holds.
  const isCrossTable = sqlEditorShape(predicate) === "query";
  // Each table the rule's query joins gets its own manual grid, so a cross-table
  // rule can be exercised on fabricated rows without creating any table. Read off
  // the query text, since that is where a joined table is named — and only for a
  // query-shaped rule, because that is the only shape the backend swaps grids into.
  const refTables = useMemo(
    () => (isCrossTable ? findReferenceTables(predicate) : []),
    [isCrossTable, predicate],
  );
  const [mode, setMode] = useState<Mode>("adhoc");
  const [result, setResult] = useState<RuleTestRunOut | null>(null);
  const [tablePayload, setTablePayload] = useState<TableSourcePayload | null>(null);
  const [sampleKind, setSampleKind] = useState<SampleKind>("records");
  const [sampleValue, setSampleValue] = useState(1000);
  const [generating, setGenerating] = useState(false);

  const famBySlot = useMemo<Record<string, string>>(
    () => Object.fromEntries(slots.map((s) => [s.name, s.family])),
    [slots],
  );

  // Logic hash: rebuild a fresh grid whenever the rule's testable logic changes.
  const logicHash = JSON.stringify({
    p: predicate,
    pol: polarity,
    s: slots.map((s) => [s.name, s.family]),
    fn: nativeFunction,
    args: nativeArguments,
  });
  const [manual, setManualState] = useState<ManualState>(
    () => getManual(CACHE_KEY, logicHash) ?? buildInitialManual(slots, predicate, refTables),
  );

  const runMutation = useRunRuleTest();
  const generateMutation = useGenerateRuleTestData();
  const isPending = runMutation.isPending;

  // Editing manual data invalidates the previous verdicts; also persist so the
  // rows survive a tab switch (the panel unmounts on Tabs change).
  const setManualAndReset = (s: ManualState) => {
    setResult(null);
    setManualState(s);
    setManual(CACHE_KEY, logicHash, s);
  };

  // Rebuild a fresh grid if the rule logic changes while the tab is mounted.
  const prevHash = useRef(logicHash);
  useEffect(() => {
    if (prevHash.current === logicHash) return;
    prevHash.current = logicHash;
    const fresh = buildInitialManual(slots, predicate, refTables);
    setManualState(fresh);
    setManual(CACHE_KEY, logicHash, fresh);
    setResult(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [logicHash]);

  const switchMode = (m: Mode) => {
    if (m === mode) return;
    setResult(null);
    setTablePayload(null);
    setMode(m);
  };

  const setCell = (ri: number, ci: number, v: string | null) => {
    const rows = manual.rows.map((r) => [...r]);
    rows[ri][ci] = v;
    setManualAndReset({ ...manual, rows });
  };
  const addRow = () => setManualAndReset({ ...manual, rows: [...manual.rows, manual.columns.map(() => null)] });
  const removeRow = (ri: number) => setManualAndReset({ ...manual, rows: manual.rows.filter((_, i) => i !== ri) });

  // Reference grids: the author owns both the rows AND the columns here, since
  // these mirror a real table's shape rather than the rule's slots.
  const patchRef = (table: string, patch: (g: RefGridState) => RefGridState) => {
    const current = manual.refs[table] ?? { columns: [], rows: [], families: {} };
    setManualAndReset({ ...manual, refs: { ...manual.refs, [table]: patch(current) } });
  };
  const refHandlers = (table: string) => ({
    onCellChange: (ri: number, ci: number, v: string | null) =>
      patchRef(table, (g) => {
        const rows = g.rows.map((r) => [...r]);
        rows[ri][ci] = v;
        return { ...g, rows };
      }),
    onAddRow: () => patchRef(table, (g) => ({ ...g, rows: [...g.rows, g.columns.map(() => null)] })),
    onDeleteRow: (ri: number) => patchRef(table, (g) => ({ ...g, rows: g.rows.filter((_, i) => i !== ri) })),
    onAddColumn: () =>
      patchRef(table, (g) => {
        const name = nextRefColumnName(g.columns);
        return {
          columns: [...g.columns, name],
          rows: g.rows.length ? g.rows.map((r) => [...r, null]) : [[null]],
          families: { ...g.families, [name]: "any" },
        };
      }),
    onDeleteColumn: (ci: number) =>
      patchRef(table, (g) => ({
        ...g,
        columns: g.columns.filter((_, i) => i !== ci),
        rows: g.rows.map((r) => r.filter((_, i) => i !== ci)),
      })),
    onRenameColumn: (ci: number, name: string) =>
      patchRef(table, (g) => {
        const columns = g.columns.map((c, i) => (i === ci ? name : c));
        const family = g.families[g.columns[ci]] ?? "any";
        return { ...g, columns, families: { ...g.families, [name]: family } };
      }),
    onRetypeColumn: (ci: number, family: CellFamily) =>
      patchRef(table, (g) => ({ ...g, families: { ...g.families, [g.columns[ci]]: family } })),
  });

  // A reference grid with no columns can't stand in for its table: the join
  // would compare against nothing and every row would silently "pass".
  const unfilledRefs = refTables.filter((tbl) => !(manual.refs[tbl]?.columns.length ?? 0));
  const adhocReady = manual.rows.length > 0 && unfilledRefs.length === 0;
  const canRun = mode === "adhoc" ? adhocReady : !!tablePayload;

  const onGenerate = () => {
    setGenerating(true);
    const columns = manual.columns.map((c) => ({ name: c, family: famBySlot[c] ?? "any" }));
    // Naming the reference tables asks for a mix that is consistent ACROSS grids
    // (some input rows matching a reference row, some deliberately not) — without
    // that, a cross-table rule's generated data can't produce both verdicts.
    const generateBody =
      ruleMode === "dqx_native"
        ? {
            function: nativeFunction,
            native_arguments: nativeArguments,
            polarity,
            row_count: 8,
            columns,
            ref_tables: refTables,
          }
        : { predicate, polarity, row_count: 8, columns, ref_tables: refTables };
    generateMutation.mutate(
      {
        data: generateBody,
      },
      {
        onSuccess: (res) => {
          setResult(null);
          // Keep any grid the model declined to fill, so a partial answer doesn't
          // wipe rows the author typed by hand.
          const refs: Record<string, RefGridState> = { ...manual.refs };
          for (const [name, grid] of Object.entries(res.data.refs ?? {})) {
            if (!grid.columns.length) continue;
            refs[name] = {
              columns: grid.columns.map((c) => c.name),
              rows: grid.rows.map((r) => [...r]),
              families: Object.fromEntries(grid.columns.map((c) => [c.name, c.family ?? "any"])),
            };
          }
          const next: ManualState = { columns: res.data.columns, rows: res.data.rows, refs };
          setManualState(next);
          setManual(CACHE_KEY, logicHash, next);
          toast.success(t("ruleTest.generateSuccess"));
        },
        onError: (err: unknown) => {
          const reason = aiUnavailableReason(err);
          if (reason) ai.reportUnavailable(reason);
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          toast.error(detail ?? t("ruleTest.generateError"));
        },
        onSettled: () => setGenerating(false),
      },
    );
  };

  const onRun = () => {
    const common = {
      mode: ruleMode,
      predicate: ruleMode === "dqx_native" ? "" : predicate,
      function: ruleMode === "dqx_native" ? nativeFunction : undefined,
      native_arguments: ruleMode === "dqx_native" ? nativeArguments : undefined,
      polarity,
      lowcode_advanced: lowcodeAdvanced,
    };
    if (mode === "adhoc") {
      runMutation.mutate(
        {
          data: {
            ...common,
            slots: slots.map((s) => ({ name: s.name, family: s.family })),
            source_kind: "adhoc",
            adhoc: {
              columns: manual.columns,
              rows: manual.rows,
              ref_grids: Object.fromEntries(
                refTables
                  .map((tbl) => [tbl, manual.refs[tbl]] as const)
                  .filter(([, g]) => !!g)
                  .map(([tbl, g]) => [tbl, { columns: g.columns, rows: g.rows, families: g.families }]),
              ),
            },
          },
        },
        { onSuccess: (res) => setResult(res.data), onError: (err) => toast.error(runError(err, t)) },
      );
    } else if (tablePayload) {
      runMutation.mutate(
        {
          data: {
            ...common,
            slots: slots.map((s) => ({ name: s.name, family: s.family })),
            source_kind: "table",
            table: {
              table_fqn: tablePayload.table,
              column_mapping: tablePayload.column_mapping,
              sample_kind: sampleKind,
              sample_value: sampleValue,
            },
          },
        },
        { onSuccess: (res) => setResult(res.data), onError: (err) => toast.error(runError(err, t)) },
      );
    }
  };

  // A query-shaped rule decides for itself which rows come back (it may
  // aggregate to one, or filter to violations only), so verdicts can't be tinted
  // onto input rows — the query's own output is shown instead.
  const resultIsGrid = mode === "table" || isCrossTable;

  // Manual verdicts: map each result row's row_idx -> passed. A row absent from
  // the result is treated as passing (green), matching dqlake.
  const manualVerdicts = useMemo(() => {
    if (resultIsGrid || !result) return undefined;
    const byIdx = new Map<number, boolean>();
    for (const r of result.rows) if (r.row_idx != null) byIdx.set(r.row_idx, r.passed);
    return manual.rows.map((_, i) => (byIdx.has(i) ? byIdx.get(i)! : true));
  }, [resultIsGrid, result, manual.rows]);

  const manualColumns: ManualColumn[] = manual.columns.map((c) => ({
    name: `{{${c}}}`,
    family: famBySlot[c] ?? "any",
  }));

  const mappedSet = useMemo(
    () => new Set(mode === "table" ? (tablePayload?.mappedColumns ?? []) : manual.columns),
    [mode, tablePayload, manual.columns],
  );
  const tableColumns: ResultColumn[] = (result?.columns ?? []).map((name) => ({ name, mapped: mappedSet.has(name) }));
  const tableRows = useMemo(
    () => (result?.rows ?? []).map((r) => tableColumns.map((c) => (r.cells[c.name] == null ? null : String(r.cells[c.name])))),
    [result, tableColumns],
  );

  if (!canTest) {
    return (
      <div className="rounded-lg border p-6 text-sm text-muted-foreground">{t("ruleTest.addDefinitionToTest")}</div>
    );
  }

  return (
    <div className="space-y-4 max-w-4xl">
      <div className="flex items-center justify-between">
        <div className="inline-flex border rounded overflow-hidden text-xs">
          <button
            type="button"
            onClick={() => switchMode("adhoc")}
            className={cn("h-8 px-3", mode === "adhoc" ? "bg-primary text-primary-foreground" : "bg-background hover:bg-accent")}
          >
            {t("ruleTest.manualTest")}
          </button>
          <button
            type="button"
            onClick={() => switchMode("table")}
            className={cn("h-8 px-3 border-l", mode === "table" ? "bg-primary text-primary-foreground" : "bg-background hover:bg-accent")}
          >
            {t("ruleTest.tableTest")}
          </button>
        </div>
      </div>

      {mode === "adhoc" ? (
        <div className="space-y-4">
          {/* Label the grids only when there is more than one, so a plain
              single-table rule keeps its uncluttered look. */}
          {refTables.length > 0 && (
            <p className="text-xs font-medium text-muted-foreground">{t("ruleTest.inputGridLabel")}</p>
          )}
          <ManualGrid
            columns={manualColumns}
            rows={manual.rows}
            verdicts={manualVerdicts}
            onCellChange={setCell}
            onAddRow={addRow}
            onDeleteRow={removeRow}
          />
          {refTables.map((tbl) => {
            const grid = manual.refs[tbl] ?? { columns: [], rows: [], families: {} };
            return (
              <div key={tbl} className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground">
                  {t("ruleTest.refGridLabel")} <code className="font-mono">{tbl}</code>
                </p>
                {grid.columns.length === 0 && (
                  <p className="text-xs text-muted-foreground">{t("ruleTest.refGridEmpty")}</p>
                )}
                <ManualGrid
                  columns={grid.columns.map((c) => ({ name: c, family: (grid.families[c] ?? "any") as CellFamily }))}
                  rows={grid.rows}
                  {...refHandlers(tbl)}
                />
              </div>
            );
          })}
          {isCrossTable && <p className="text-xs text-muted-foreground">{t("ruleTest.manualQueryResultNote")}</p>}
        </div>
      ) : (
        <div className="space-y-3">
          <TableTestSource slots={slots} onReady={setTablePayload} />
          {/* A cross-table rule runs as a query, so the grid shows the rows the
              QUERY returns rather than every sampled row — say so up front, or a
              rule that filters to violations looks like it lost rows. */}
          {isCrossTable && <p className="text-xs text-muted-foreground">{t("ruleTest.crossTableResultNote")}</p>}
        </div>
      )}

      {/* "Generate test data" sits BETWEEN the manual-entry grid and the Run
          test button (not off to the side) — the natural step: fill the grid
          with AI-generated rows, then run. Each control is wrapped in its own
          block-level flex row so the (inline-flex) buttons stack vertically
          and left-align instead of floating up beside the grid. */}
      {mode === "adhoc" && (
        <div className="flex">
          <DisabledTooltip disabled={!ai.available} label={ai.reason ?? t("ruleTest.aiDisabled")}>
            <Button variant="outline" onClick={onGenerate} disabled={!ai.available || generating}>
              {generating ? (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="mr-1.5 h-4 w-4" stroke={AI_GRADIENT_URL} />
              )}
              {t("ruleTest.generateData")}
            </Button>
          </DisabledTooltip>
        </div>
      )}

      <div className="flex items-center justify-between">
        <WarehouseWaitTooltip show={!warehouseReady} label={t("ruleTest.waitingForWarehouse")}>
          <DisabledTooltip
            disabled={mode === "adhoc" && unfilledRefs.length > 0}
            label={t("ruleTest.refGridsIncomplete", { names: unfilledRefs.join(", ") })}
          >
            <Button onClick={onRun} disabled={!canRun || isPending || !warehouseReady}>
              {isPending && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
              {isPending ? t("ruleTest.running") : t("ruleTest.runTest")}
            </Button>
          </DisabledTooltip>
        </WarehouseWaitTooltip>
        {mode === "table" && (
          <SampleSelector
            kind={sampleKind}
            value={sampleValue}
            onKind={(k) => {
              setSampleKind(k);
              if (k === "percent") setSampleValue(10);
              else if (k === "records") setSampleValue(1000);
            }}
            onValue={setSampleValue}
          />
        )}
      </div>

      {resultIsGrid &&
        (mode === "adhoc" || tablePayload) &&
        result &&
        (result.rows.length === 0 ? (
          <p className="text-sm text-muted-foreground">{t("ruleTest.noRows")}</p>
        ) : (
          <ResultGrid columns={tableColumns} rows={tableRows} verdicts={result.rows.map((r) => r.passed)} />
        ))}

      {result?.truncated && (
        <p className="text-xs text-muted-foreground">
          {t("ruleTest.truncatedNote", { count: result.rows.length })}
        </p>
      )}
    </div>
  );
}

export type { SampleKind };

export function SampleSelector({
  kind,
  value,
  onKind,
  onValue,
  disablePercent = false,
}: {
  kind: SampleKind;
  value: number;
  onKind: (k: SampleKind) => void;
  onValue: (n: number) => void;
  /** When true, hides the "percent" unit option so only records/full are available.
   *  Use in contexts where the underlying setting is rows-only (e.g. draft_sample_limit). */
  disablePercent?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex items-center gap-1.5">
      <Select value={kind === "full" ? "full" : "sample"} onValueChange={(v) => onKind(v === "full" ? "full" : "records")}>
        <SelectTrigger className="h-8 w-36 text-xs font-normal text-muted-foreground">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="sample">{t("ruleTest.randomSample")}</SelectItem>
          <SelectItem value="full">{t("ruleTest.fullTable")}</SelectItem>
        </SelectContent>
      </Select>
      {kind !== "full" && (
        <>
          <Input
            type="number"
            min={1}
            max={kind === "percent" ? 100 : undefined}
            className="h-8 w-24 text-xs font-normal text-muted-foreground"
            value={value}
            onChange={(e) => {
              const n = Number(e.target.value);
              onValue(kind === "percent" ? Math.min(100, Math.max(1, n)) : n);
            }}
          />
          {!disablePercent ? (
            <Select value={kind} onValueChange={(v) => onKind(v as "records" | "percent")}>
              <SelectTrigger className="h-8 w-28 text-xs font-normal text-muted-foreground">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="records">{t("ruleTest.records")}</SelectItem>
                <SelectItem value="percent">{t("ruleTest.percent")}</SelectItem>
              </SelectContent>
            </Select>
          ) : (
            <span className="text-xs text-muted-foreground w-28 px-1">{t("ruleTest.records")}</span>
          )}
        </>
      )}
    </div>
  );
}

function runError(err: unknown, t: (k: string) => string): string {
  const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
  return detail ?? t("ruleTest.runError");
}
