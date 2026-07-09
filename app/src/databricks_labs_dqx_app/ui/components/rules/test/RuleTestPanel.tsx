// RuleTestPanel — the Rules Registry "Test" tab surface (P22-E). Ports
// dqlake's `test/TestRuleTab.tsx` in spirit, adapted to DQX's registry rule
// model: a rule is tested by evaluating its effective SQL predicate over manual
// rows (inline VALUES) or a real UC table sample on the configured warehouse
// (OBO). Only `sql` / `lowcode` rules reach here — `dqx_native` shows the
// "not available" notice in the parent.
//
// Subtle behaviours carried over from dqlake:
//  - Manual / Table mode toggle; switching resets the last result.
//  - Manual grid: typed cells per slot family, add/edit/delete rows, Enter to
//    add+focus a new row, green/red row tinting after a run.
//  - "Generate test data" (AI) fills a passing/failing mix; gated + degrades
//    when AI is off.
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
import {
  useRunRuleTest,
  useGenerateRuleTestData,
  type RuleSlot,
  type RuleTestRunOut,
} from "@/lib/api";
import { ManualGrid, type ManualColumn } from "./ManualGrid";
import { ResultGrid, type ResultColumn } from "./ResultGrid";
import { TableTestSource, type TableSourcePayload } from "./TableTestSource";
import { useWarehousePrewarm } from "./useWarehousePrewarm";
import { getManual, setManual, type ManualState } from "./manualCache";

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

function AiDisabledTooltip({ disabled, label, children }: { disabled: boolean; label: string; children: React.ReactNode }) {
  if (!disabled) return <>{children}</>;
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex">{children}</span>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

function buildInitialManual(slots: RuleSlot[]): ManualState {
  const columns = slots.length ? slots.map((s) => s.name) : ["value"];
  return { columns, rows: [columns.map(() => null)] };
}

const CACHE_KEY = "registry-rule-test";

export function RuleTestPanel({
  predicate,
  polarity,
  slots,
  canTest,
}: {
  /** Effective SQL predicate: raw in SQL mode, compiled AST in Low-Code mode. */
  predicate: string;
  polarity: "pass" | "fail";
  slots: RuleSlot[];
  /** Whether there is a non-empty predicate to test against. */
  canTest: boolean;
}) {
  const { t } = useTranslation();
  const ai = useAiAvailability();
  const { ready: warehouseReady } = useWarehousePrewarm();

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
  const logicHash = JSON.stringify({ p: predicate, pol: polarity, s: slots.map((s) => [s.name, s.family]) });
  const [manual, setManualState] = useState<ManualState>(
    () => getManual(CACHE_KEY, logicHash) ?? buildInitialManual(slots),
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
    const fresh = buildInitialManual(slots);
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

  const adhocReady = manual.rows.length > 0;
  const canRun = mode === "adhoc" ? adhocReady : !!tablePayload;

  const onGenerate = () => {
    setGenerating(true);
    generateMutation.mutate(
      {
        data: {
          predicate,
          polarity,
          row_count: 8,
          columns: manual.columns.map((c) => ({ name: c, family: famBySlot[c] ?? "any" })),
        },
      },
      {
        onSuccess: (res) => {
          setResult(null);
          const next: ManualState = { columns: res.data.columns, rows: res.data.rows };
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
    const common = { mode: "sql" as const, predicate, polarity };
    if (mode === "adhoc") {
      runMutation.mutate(
        {
          data: {
            ...common,
            slots: slots.map((s) => ({ name: s.name, family: s.family })),
            source_kind: "adhoc",
            adhoc: { columns: manual.columns, rows: manual.rows },
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

  // Manual verdicts: map each result row's row_idx -> passed. A row absent from
  // the result is treated as passing (green), matching dqlake.
  const manualVerdicts = useMemo(() => {
    if (mode !== "adhoc" || !result) return undefined;
    const byIdx = new Map<number, boolean>();
    for (const r of result.rows) if (r.row_idx != null) byIdx.set(r.row_idx, r.passed);
    return manual.rows.map((_, i) => (byIdx.has(i) ? byIdx.get(i)! : true));
  }, [mode, result, manual.rows]);

  const manualColumns: ManualColumn[] = manual.columns.map((c) => ({
    name: `{{${c}}}`,
    family: famBySlot[c] ?? "any",
  }));

  const mappedSet = useMemo(() => new Set(tablePayload?.mappedColumns ?? []), [tablePayload]);
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
        {mode === "adhoc" && (
          <AiDisabledTooltip disabled={!ai.available} label={ai.reason ?? t("ruleTest.aiDisabled")}>
            <Button variant="outline" size="sm" onClick={onGenerate} disabled={!ai.available || generating}>
              {generating ? (
                <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
              ) : (
                <Sparkles className="mr-1.5 h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />
              )}
              {t("ruleTest.generateData")}
            </Button>
          </AiDisabledTooltip>
        )}
      </div>

      {mode === "adhoc" ? (
        <ManualGrid
          columns={manualColumns}
          rows={manual.rows}
          verdicts={manualVerdicts}
          onCellChange={setCell}
          onAddRow={addRow}
          onDeleteRow={removeRow}
        />
      ) : (
        <TableTestSource slots={slots} onReady={setTablePayload} />
      )}

      <div className="flex items-center justify-between">
        <WarehouseWaitTooltip show={!warehouseReady} label={t("ruleTest.waitingForWarehouse")}>
          <Button onClick={onRun} disabled={!canRun || isPending || !warehouseReady}>
            {isPending && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
            {isPending ? t("ruleTest.running") : t("ruleTest.runTest")}
          </Button>
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

      {mode === "table" &&
        tablePayload &&
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

function SampleSelector({
  kind,
  value,
  onKind,
  onValue,
}: {
  kind: SampleKind;
  value: number;
  onKind: (k: SampleKind) => void;
  onValue: (n: number) => void;
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
          <Select value={kind} onValueChange={(v) => onKind(v as "records" | "percent")}>
            <SelectTrigger className="h-8 w-28 text-xs font-normal text-muted-foreground">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="records">{t("ruleTest.records")}</SelectItem>
              <SelectItem value="percent">{t("ruleTest.percent")}</SelectItem>
            </SelectContent>
          </Select>
        </>
      )}
    </div>
  );
}

function runError(err: unknown, t: (k: string) => string): string {
  const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
  return detail ?? t("ruleTest.runError");
}
