// THROWAWAY prototype — right-hand "Live impact" pane for variant C.
// Recomputed from mockPreview(draft) with a simulated debounce by the parent.
import { useMemo, useState, type ReactNode } from "react";
import { ArrowRight, ChevronRight, Loader2, Radar, Table2 } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import type { ColumnCondition, PolicyDraft, PolicyPreview, PreviewAttachment, PreviewTable } from "../mock";
import { Pill, joinList, ruleById, slotCondition } from "./shared";

type Filter = "all" | "attach" | "review" | "skipped";

function Delta({ now, prev }: { now: number; prev: number | undefined }) {
  if (prev === undefined || prev === now) return null;
  const d = now - prev;
  return (
    <span
      className={cn(
        "rounded px-1 text-[10px] font-semibold tabular-nums",
        d > 0 ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground",
      )}
    >
      {d > 0 ? `+${d}` : `−${Math.abs(d)}`}
    </span>
  );
}

function Stat({ label, value, prev, hint, tone }: { label: string; value: number; prev?: number; hint?: string; tone?: "warn" | "muted" }) {
  return (
    <div className="rounded-md border bg-background/60 px-3 py-2">
      <div className="flex items-center gap-1.5">
        <span
          className={cn(
            "text-xl font-semibold tabular-nums",
            tone === "warn" && value > 0 && "text-amber-600 dark:text-amber-400",
            (tone === "muted" || value === 0) && "text-muted-foreground",
          )}
        >
          {value}
        </span>
        <Delta now={value} prev={prev} />
      </div>
      <div className="text-[11px] font-medium leading-tight">{label}</div>
      {hint && <div className="text-[10px] leading-tight text-muted-foreground">{hint}</div>}
    </div>
  );
}

function condText(c: ColumnCondition): string {
  if (c.kind === "tags") return c.tags.length ? `tagged ${joinList(c.tags, "or")}` : "tagged (no tags picked yet)";
  return "matching a custom condition";
}

function Summary({ draft }: { draft: PolicyDraft }) {
  const rules = draft.rule_ids.map((id) => ruleById(id)?.name ?? id);
  const scope = draft.scope.filter((s) => s.catalog);
  if (rules.length === 0 || scope.length === 0) {
    return <p className="text-xs text-muted-foreground">Add at least one rule and a scope catalog to see what this policy does.</p>;
  }
  const B = ({ children }: { children: ReactNode }) => <span className="font-medium text-foreground">{children}</span>;
  const ruleText = rules.length > 3 ? `${rules.length} rules` : joinList(rules.map((r) => `“${r}”`));
  const slotted = draft.rule_ids.flatMap((id) => (ruleById(id)?.slots ?? []).map((s) => condText(slotCondition(draft, id, s.name))));
  const uniq = [...new Set(slotted)];
  const target =
    slotted.length === 0 ? "every table" : uniq.length === 1 ? `columns ${uniq[0]}` : "columns that match each rule's column condition";
  const scopeText = joinList(scope.map((s) => (s.schema === "*" ? `${s.catalog} (all schemas)` : `${s.catalog}.${s.schema}`)));
  const tc = draft.table_condition;
  const tableText =
    tc.kind === "none" ? "" : tc.kind === "tags" ? `, in tables tagged ${tc.tags.length ? joinList(tc.tags, "or") : "…"}` : ", in tables matching a custom condition";
  return (
    <p className="text-xs leading-relaxed text-muted-foreground">
      {draft.action === "auto_apply" ? "Attaches " : "Suggests "}
      <B>{ruleText}</B> to <B>{target}</B> in <B>{scopeText}</B>
      {tableText}.{draft.register_tables && " Unmonitored matches are registered as draft, unscheduled tables."}
      {draft.action === "suggest" && " Nothing is attached until someone accepts a suggestion."}
    </p>
  );
}

function StatusBadge({ a, suggest }: { a: PreviewAttachment; suggest: boolean }) {
  if (a.status === "ambiguous") return <Pill tone="amber">Ambiguous</Pill>;
  if (a.status === "partial") return <Pill tone="muted">Partial</Pill>;
  return suggest ? <Pill tone="sky">Suggest</Pill> : <Pill tone="emerald">Auto</Pill>;
}

function AttachmentRow({ a, suggest }: { a: PreviewAttachment; suggest: boolean }) {
  const rule = ruleById(a.rule_id);
  const entries = Object.entries(a.mapping);
  return (
    <li className="space-y-1 py-1.5">
      <div className="flex items-center gap-2">
        <span className="min-w-0 flex-1 truncate text-xs font-medium">{rule?.name ?? a.rule_id}</span>
        {rule && rule.status !== "approved" && <Pill tone="muted">Once approved</Pill>}
        <StatusBadge a={a} suggest={suggest} />
      </div>
      <div className="flex flex-wrap items-center gap-1">
        {entries.length === 0 ? (
          <span className="text-[11px] text-muted-foreground">Table-level, no columns</span>
        ) : (
          entries.map(([slot, cols]) => (
            <span key={slot} className="inline-flex items-center gap-1 rounded border bg-muted/40 px-1.5 py-0.5 font-mono text-[10px]">
              <span className="text-muted-foreground">{`{{${slot}}}`}</span>
              <ArrowRight className="h-2.5 w-2.5 text-muted-foreground" />
              <span>{cols.join(", ")}</span>
            </span>
          ))
        )}
        {rule &&
          a.status === "partial" &&
          rule.slots
            .filter((s) => !a.mapping[s.name])
            .map((s) => (
              <span key={s.name} className="inline-flex items-center gap-1 rounded border border-dashed px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground">
                {`{{${s.name}}}`} <ArrowRight className="h-2.5 w-2.5" /> ?
              </span>
            ))}
      </div>
      <div className="text-[11px] text-muted-foreground">{a.reason}</div>
    </li>
  );
}

function TableRowItem({ t, suggest, isNew }: { t: PreviewTable; suggest: boolean; isNew: boolean }) {
  const [open, setOpen] = useState(false);
  const name = t.table_fqn.split(".").slice(2).join(".");
  const auto = t.attachments.filter((a) => a.status === "auto").length;
  const review = t.attachments.filter((a) => a.status !== "auto").length;
  return (
    <li className={cn("rounded-md transition-colors", isNew && "bg-primary/5 ring-1 ring-primary/20")}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className={cn("flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-muted/50", t.skipped_reason && "opacity-70")}
      >
        <ChevronRight className={cn("h-3.5 w-3.5 shrink-0 text-muted-foreground transition-transform", open && "rotate-90")} />
        <Table2 className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
        <span className={cn("min-w-0 flex-1 truncate font-mono text-xs", t.skipped_reason && "line-through decoration-muted-foreground/50")}>{name}</span>
        {isNew && <span className="text-[10px] font-semibold text-primary">new</span>}
        {t.skipped_reason ? (
          <Pill tone="muted">Skipped</Pill>
        ) : (
          <>
            {!t.monitored && <Pill tone="primary">Will register</Pill>}
            {review > 0 && <Pill tone="amber">{review} to review</Pill>}
            <span className="w-14 text-right text-[11px] tabular-nums text-muted-foreground">
              {auto} {suggest ? "sugg." : "attach"}
            </span>
          </>
        )}
      </button>
      {open && (
        <div className="ml-7 border-l pb-1 pl-3">
          {t.skipped_reason && <p className="py-1.5 text-[11px] text-muted-foreground">{t.skipped_reason} on this table for the policy owner. Nothing will be attached.</p>}
          {!t.monitored && !t.skipped_reason && (
            <p className="py-1 text-[11px] text-muted-foreground">Not monitored yet. Registered as a draft, unscheduled table.</p>
          )}
          <ul className="divide-y">
            {t.attachments.map((a, i) => (
              <AttachmentRow key={i} a={a} suggest={suggest} />
            ))}
          </ul>
        </div>
      )}
    </li>
  );
}

export function ImpactPane({
  draft,
  preview,
  prev,
  loading,
}: {
  draft: PolicyDraft;
  preview: PolicyPreview;
  prev: PolicyPreview | null;
  loading: boolean;
}) {
  const [filter, setFilter] = useState<Filter>("all");
  const suggest = draft.action === "suggest";
  const prevFqns = useMemo(() => (prev ? new Set(prev.tables.map((t) => t.table_fqn)) : null), [prev]);
  const removed = prev ? prev.tables.filter((t) => !preview.tables.some((n) => n.table_fqn === t.table_fqn)).length : 0;

  const visible = preview.tables.filter((t) => {
    if (filter === "skipped") return !!t.skipped_reason;
    if (t.skipped_reason) return filter === "all";
    if (filter === "attach") return t.attachments.some((a) => a.status === "auto");
    if (filter === "review") return t.attachments.some((a) => a.status !== "auto");
    return true;
  });
  const groups = useMemo(() => {
    const m = new Map<string, PreviewTable[]>();
    for (const t of visible) {
      const k = t.table_fqn.split(".").slice(0, 2).join(".");
      m.set(k, [...(m.get(k) ?? []), t]);
    }
    return [...m.entries()];
  }, [visible]);
  const review = preview.tables.filter((t) => !t.skipped_reason && t.attachments.some((a) => a.status !== "auto")).length;
  const drafts = draft.rule_ids.map((id) => ruleById(id)).filter((r) => r && r.status !== "approved");

  const FILTERS: { key: Filter; label: string; n: number }[] = [
    { key: "all", label: "All", n: preview.tables.length },
    { key: "attach", label: suggest ? "Suggest" : "Attach", n: preview.tables.filter((t) => !t.skipped_reason && t.attachments.some((a) => a.status === "auto")).length },
    { key: "review", label: "Review", n: review },
    { key: "skipped", label: "Skipped", n: preview.skipped },
  ];

  return (
    <div className="flex h-full min-h-0 flex-col rounded-lg border bg-card">
      <div className="flex items-center gap-2 border-b px-4 py-3">
        <Radar className="h-4 w-4 text-primary" />
        <div className="text-sm font-semibold">Live impact</div>
        <span className="ml-auto inline-flex items-center gap-1.5 text-[11px] text-muted-foreground">
          {loading ? (
            <>
              <Loader2 className="h-3 w-3 animate-spin" /> Updating
            </>
          ) : (
            "Dry run · nothing is saved"
          )}
        </span>
      </div>

      <div className={cn("space-y-3 border-b px-4 py-3 transition-opacity", loading && "opacity-60")}>
        <div className="grid grid-cols-3 gap-2">
          <Stat label="Monitored tables" value={preview.matched_monitored} prev={prev?.matched_monitored} />
          <Stat label={suggest ? "Suggestions" : "Attachments"} value={preview.attachments} prev={prev?.attachments} />
          <Stat
            label="To register"
            value={preview.to_register}
            prev={prev?.to_register}
            hint={draft.register_tables ? "Draft, unscheduled" : "Monitoring off"}
            tone={draft.register_tables ? undefined : "muted"}
          />
          <Stat label="Skipped" value={preview.skipped} prev={prev?.skipped} hint="No APPLY or MANAGE" tone="muted" />
          <Stat label="Ambiguous" value={preview.ambiguous} prev={prev?.ambiguous} hint="Will be suggested" tone="warn" />
          <Stat label="Tables to review" value={review} hint="Ambiguous or partial" tone="warn" />
        </div>
        {loading ? (
          <div className="space-y-1.5">
            <Skeleton className="h-3 w-full" />
            <Skeleton className="h-3 w-4/5" />
          </div>
        ) : (
          <Summary draft={draft} />
        )}
        {drafts.length > 0 && (
          <p className="text-[11px] text-muted-foreground">
            {joinList(drafts.map((r) => `“${r!.name}”`))} {drafts.length === 1 ? "is" : "are"} not approved yet; matches attach once approved.
          </p>
        )}
        {removed > 0 && !loading && (
          <p className="text-[11px] text-muted-foreground">
            Last change removed {removed} table{removed === 1 ? "" : "s"} from the match. Existing attachments stay; policies only add.
          </p>
        )}
      </div>

      <div className="flex items-center gap-1 border-b px-3 py-2">
        {FILTERS.map((f) => (
          <button
            key={f.key}
            type="button"
            onClick={() => setFilter(f.key)}
            className={cn(
              "rounded-md px-2 py-1 text-[11px] font-medium transition-colors",
              filter === f.key ? "bg-primary/10 text-primary" : "text-muted-foreground hover:text-foreground",
            )}
          >
            {f.label} <span className="tabular-nums opacity-70">{f.n}</span>
          </button>
        ))}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-2 py-2">
        {loading && preview.tables.length === 0 ? (
          <div className="space-y-2 p-2">
            {[0, 1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-7 w-full" />
            ))}
          </div>
        ) : groups.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center gap-1 px-6 py-10 text-center">
            <Table2 className="h-6 w-6 text-muted-foreground/60" />
            <p className="text-xs text-muted-foreground">
              {preview.tables.length === 0 ? "No tables match yet. Pick a rule and a scope catalog." : "No tables in this filter."}
            </p>
          </div>
        ) : (
          <div className={cn("space-y-3 transition-opacity", loading && "opacity-60")}>
            {groups.map(([schema, tables]) => (
              <div key={schema}>
                <div className="sticky top-0 z-10 flex items-center justify-between bg-card px-2 py-1 font-mono text-[11px] font-semibold text-muted-foreground">
                  <span>{schema}</span>
                  <span className="font-sans font-normal tabular-nums">{tables.length}</span>
                </div>
                <ul className="space-y-0.5">
                  {tables.map((t) => (
                    <TableRowItem key={t.table_fqn} t={t} suggest={suggest} isNew={!!prevFqns && !prevFqns.has(t.table_fqn)} />
                  ))}
                </ul>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
