// THROWAWAY prototype — wizard step bodies for the wizard-sheet variant (B).
import { useState, type ReactNode } from "react";
import {
  Ban,
  Braces,
  ChevronDown,
  ChevronRight,
  Copy,
  Lightbulb,
  Pencil,
  Plus,
  Table2,
  Tags,
  Trash2,
  X,
  Zap,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import {
  COLUMN_SUBJECTS,
  MOCK_CATALOGS,
  MOCK_GOVERNED_TAGS,
  TABLE_SUBJECTS,
  mockPreview,
  type ColumnCondition,
  type PolicyDraft,
  type PreviewAttachment,
  type PreviewTable,
  type TableCondition,
} from "../mock";
import {
  AddRulesDialog,
  ChoiceTiles,
  ConditionBuilder,
  FieldError,
  InfoLine,
  RuleStatusBadge,
  Section,
  TagChip,
  TagMultiPicker,
  emptyExpr,
} from "./controls";
import {
  columnCondition,
  conditionDetail,
  hasSlots,
  ruleById,
  scopeText,
  selectedRules,
  slotLabel,
  summarize,
  withColumnCondition,
  type Errors,
  type StepKey,
} from "./model";

interface StepProps {
  draft: PolicyDraft;
  setDraft: (d: PolicyDraft) => void;
  errors: Errors;
}

// ── Step 1: Rules ───────────────────────────────────────────────────────

export function StepRules({ draft, setDraft, errors }: StepProps) {
  const [pickerOpen, setPickerOpen] = useState(false);
  const rules = selectedRules(draft);
  return (
    <div className="space-y-8">
      <Section title="Name" required description="Shown on the Policies tab of every rule in this policy.">
        <div className="space-y-1.5">
          <Input
            value={draft.name}
            onChange={(e) => setDraft({ ...draft, name: e.target.value })}
            placeholder="e.g. PII emails in prod"
            aria-invalid={!!errors.name}
            autoFocus
          />
          <FieldError message={errors.name} />
        </div>
        <Textarea
          value={draft.description}
          onChange={(e) => setDraft({ ...draft, description: e.target.value })}
          placeholder="Description (optional)"
          rows={2}
          className="resize-none"
        />
      </Section>

      <Section
        title="Rules"
        required
        description="Every rule in the policy is applied wherever the policy matches."
        aside={
          <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setPickerOpen(true)}>
            <Plus className="h-3.5 w-3.5" /> Add rules
          </Button>
        }
      >
        {rules.length === 0 ? (
          <div
            className={cn(
              "rounded-md border border-dashed px-4 py-6 text-center text-sm text-muted-foreground",
              errors.rules && "border-destructive",
            )}
          >
            No rules yet
          </div>
        ) : (
          <ul className="divide-y rounded-md border">
            {rules.map((r) => (
              <li key={r.rule_id} className="flex items-center gap-3 px-3 py-2">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <span className="truncate">{r.name}</span>
                    {r.status !== "approved" && <RuleStatusBadge status={r.status} />}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    <span className="capitalize">{r.dimension}</span>
                    {" · "}
                    {r.slots.length === 0 ? "whole table" : r.slots.map((s) => `{{${s.name}}}`).join(", ")}
                    {r.status !== "approved" && " · Applies once approved"}
                  </div>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0"
                  aria-label={`Remove ${r.name}`}
                  onClick={() => setDraft({ ...draft, rule_ids: draft.rule_ids.filter((id) => id !== r.rule_id) })}
                >
                  <X className="h-4 w-4" />
                </Button>
              </li>
            ))}
          </ul>
        )}
        <FieldError message={errors.rules} />
        <AddRulesDialog
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          existing={draft.rule_ids}
          onAdd={(ids) => setDraft({ ...draft, rule_ids: [...draft.rule_ids, ...ids] })}
        />
      </Section>

      <Section title="Action" description="What happens when a table or column matches.">
        <ChoiceTiles
          value={draft.action}
          onChange={(action) => setDraft({ ...draft, action })}
          options={[
            {
              value: "auto_apply",
              label: "Apply automatically",
              icon: <Zap />,
              info: "Matching rules are attached to tables straight away, marked “via policy”. Ambiguous matches are suggested instead.",
            },
            {
              value: "suggest",
              label: "Suggest only",
              icon: <Lightbulb />,
              info: "Matches show up as suggestions on each table. Nothing is attached until someone accepts them.",
            },
          ]}
        />
      </Section>
    </div>
  );
}

// ── Step 2: Where ───────────────────────────────────────────────────────

export function StepWhere({ draft, setDraft, errors }: StepProps) {
  const setScope = (i: number, patch: Partial<PolicyDraft["scope"][number]>) => {
    const scope = draft.scope.slice();
    scope[i] = { ...scope[i], ...patch };
    setDraft({ ...draft, scope });
  };
  const tc = draft.table_condition;
  const setTc = (table_condition: TableCondition) => setDraft({ ...draft, table_condition });

  return (
    <div className="space-y-8">
      <Section title="Scope" required description="Catalogs and schemas the policy looks in.">
        <div className="space-y-2">
          {draft.scope.map((s, i) => (
            <div key={i} className="space-y-1">
              <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_2rem] items-center gap-2">
                <Select value={s.catalog} onValueChange={(catalog) => setScope(i, { catalog, schema: "*" })}>
                  <SelectTrigger className="w-full" aria-invalid={!!errors[`scope.${i}`]}>
                    <SelectValue placeholder="Catalog" />
                  </SelectTrigger>
                  <SelectContent>
                    {Object.keys(MOCK_CATALOGS).map((c) => (
                      <SelectItem key={c} value={c}>
                        {c}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select value={s.schema} onValueChange={(schema) => setScope(i, { schema })} disabled={!s.catalog}>
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="*">All schemas</SelectItem>
                    {(MOCK_CATALOGS[s.catalog] ?? []).map((sc) => (
                      <SelectItem key={sc} value={sc}>
                        {sc}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="h-8 w-8 p-0"
                  disabled={draft.scope.length === 1}
                  aria-label="Remove scope"
                  onClick={() => setDraft({ ...draft, scope: draft.scope.filter((_, j) => j !== i) })}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
              <FieldError message={errors[`scope.${i}`]} />
            </div>
          ))}
          <FieldError message={errors.scope} />
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-7 text-xs"
            onClick={() => setDraft({ ...draft, scope: [...draft.scope, { catalog: "", schema: "*" }] })}
          >
            <Plus className="mr-1 h-3.5 w-3.5" /> Add scope
          </Button>
        </div>
        <label className="flex cursor-pointer items-start gap-2.5 rounded-md border bg-card px-3 py-2.5">
          <Checkbox
            checked={draft.register_tables}
            onCheckedChange={(v) => setDraft({ ...draft, register_tables: v })}
            className="mt-0.5"
          />
          <span className="space-y-0.5">
            <span className="block text-sm font-medium">Also start monitoring matching tables</span>
            <span className="block text-xs text-muted-foreground">
              Added as draft and unscheduled. Without this, only tables already monitored are matched.
            </span>
          </span>
        </label>
      </Section>

      <Section title="Table condition" description="Narrow the tables in scope.">
        <ChoiceTiles
          value={tc.kind}
          onChange={(kind) =>
            setTc(
              kind === "none"
                ? { kind: "none" }
                : kind === "tags"
                  ? { kind: "tags", tags: [] }
                  : { kind: "builder", expr: emptyExpr(TABLE_SUBJECTS) },
            )
          }
          options={[
            { value: "none", label: "No condition", icon: <Ban />, info: "Every table in scope is considered." },
            {
              value: "tags",
              label: "Tables matching selected tags",
              icon: <Tags />,
              info: "Tables with any of the selected tags. A tag key alone matches any of its values.",
            },
            {
              value: "builder",
              label: "Tables matching custom condition",
              icon: <Braces />,
              info: "Combine tags, owner and table name with AND / OR.",
            },
          ]}
        />
        {tc.kind === "tags" && (
          <TagMultiPicker value={tc.tags} onChange={(tags) => setTc({ kind: "tags", tags })} invalid={!!errors.table_condition} />
        )}
        {tc.kind === "builder" && (
          <ConditionBuilder
            expr={tc.expr}
            onChange={(expr) => setTc({ kind: "builder", expr })}
            subjects={TABLE_SUBJECTS}
            invalid={!!errors.table_condition}
          />
        )}
        <FieldError message={errors.table_condition} />
      </Section>
    </div>
  );
}

// ── Step 3: Columns ─────────────────────────────────────────────────────

export function NoColumnsNote() {
  return (
    <div className="flex items-start gap-3 rounded-md border border-dashed px-4 py-5">
      <Table2 className="mt-0.5 h-5 w-5 text-muted-foreground" />
      <div>
        <p className="text-sm font-medium">No column mapping needed</p>
        <p className="text-xs text-muted-foreground">
          These rules check whole tables — no column mapping needed. They attach once per matching table.
        </p>
      </div>
    </div>
  );
}

function ColumnConditionEditor({
  cond,
  onChange,
  error,
}: {
  cond: ColumnCondition;
  onChange: (c: ColumnCondition) => void;
  error?: string;
}) {
  return (
    <div className="space-y-2">
      <ChoiceTiles
        size="sm"
        value={cond.kind}
        onChange={(kind) =>
          onChange(kind === "tags" ? { kind: "tags", tags: [] } : { kind: "builder", expr: emptyExpr(COLUMN_SUBJECTS) })
        }
        options={[
          {
            value: "tags",
            label: "Columns matching selected tags",
            icon: <Tags />,
            info: "Columns with any of the selected tags, and a compatible data type.",
          },
          {
            value: "builder",
            label: "Columns matching custom condition",
            icon: <Braces />,
            info: "Combine column, table and catalog properties with AND / OR.",
          },
        ]}
      />
      {cond.kind === "tags" ? (
        <TagMultiPicker value={cond.tags} onChange={(tags) => onChange({ kind: "tags", tags })} invalid={!!error} />
      ) : (
        <ConditionBuilder
          expr={cond.expr}
          onChange={(expr) => onChange({ kind: "builder", expr })}
          subjects={COLUMN_SUBJECTS}
          invalid={!!error}
        />
      )}
      <FieldError message={error} />
    </div>
  );
}

export function StepColumns({ draft, setDraft, errors }: StepProps) {
  const rules = selectedRules(draft).filter((r) => r.slots.length > 0);
  const tableOnly = selectedRules(draft).filter((r) => r.slots.length === 0);
  const slotCount = rules.reduce((n, r) => n + r.slots.length, 0);
  const multi = rules.some((r) => r.slots.length >= 2);
  const [copied, setCopied] = useState<string | null>(null);

  if (rules.length === 0) return <NoColumnsNote />;

  const first = rules[0];
  const firstCond = columnCondition(draft, first.rule_id, first.slots[0].name);
  const applyToAll = () => {
    let next = draft;
    for (const r of rules) for (const s of r.slots) next = withColumnCondition(next, r.rule_id, s.name, structuredClone(firstCond));
    setDraft(next);
    setCopied(`Copied to ${slotCount} slots`);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-0.5">
          <h3 className="text-sm font-semibold">Column conditions</h3>
          <p className="text-xs text-muted-foreground">
            For each rule input, choose which columns it binds to. Only columns with a compatible type are used.
          </p>
        </div>
        {slotCount >= 2 && (
          <div className="flex shrink-0 flex-col items-end gap-1">
            <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={applyToAll}>
              <Copy className="h-3.5 w-3.5" /> Use the same condition for all rules
            </Button>
            <span className="text-[11px] text-muted-foreground">
              {copied ?? `Copies the ${`{{${first.slots[0].name}}}`} condition`}
            </span>
          </div>
        )}
      </div>

      {rules.map((rule) => (
        <div key={rule.rule_id} className="rounded-lg border bg-card">
          <div className="flex items-center gap-2 border-b px-4 py-2.5">
            <span className="text-sm font-medium">{rule.name}</span>
            <Badge variant="outline" className="capitalize">
              {rule.dimension}
            </Badge>
          </div>
          <div className="divide-y">
            {rule.slots.map((slot) => {
              const key = `col.${rule.rule_id}.${slot.name}`;
              return (
                <div key={slot.name} className="space-y-3 px-4 py-4">
                  <div className="text-sm">
                    <span className="font-mono font-medium">{`{{${slot.name}}}`}</span>
                    <span className="text-muted-foreground"> · {slotLabel(slot)}</span>
                  </div>
                  <ColumnConditionEditor
                    cond={columnCondition(draft, rule.rule_id, slot.name)}
                    onChange={(c) => {
                      setCopied(null);
                      setDraft(withColumnCondition(draft, rule.rule_id, slot.name, c));
                    }}
                    error={errors[key]}
                  />
                </div>
              );
            })}
          </div>
        </div>
      ))}

      {multi && (
        <Section
          title="Pair columns by tag"
          description="Optional. For rules with two or more inputs, decides which columns go together on a table."
        >
          <Select
            value={draft.pair_by_tag ?? "__none__"}
            onValueChange={(v) => setDraft({ ...draft, pair_by_tag: v === "__none__" ? null : v })}
          >
            <SelectTrigger className="w-64">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__none__">Don't pair by tag</SelectItem>
              {MOCK_GOVERNED_TAGS.map((t) => (
                <SelectItem key={t.key} value={t.key} className="font-mono">
                  {t.key}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <div className="space-y-1.5 rounded-md bg-muted/40 px-3 py-2.5 text-xs text-muted-foreground">
            <p>
              {draft.pair_by_tag ? (
                <>
                  Columns are paired when they share the same value for <span className="font-mono">{draft.pair_by_tag}</span>{" "}
                  (e.g. both tagged <span className="font-mono">{draft.pair_by_tag}=order</span>). Remaining columns are
                  then paired by name.
                </>
              ) : (
                <>Columns are paired by name: shared prefixes and suffixes win.</>
              )}
            </p>
            <p className="font-mono text-foreground/80">order_start_date ↔ order_end_date</p>
            <p>If a pairing isn't clear, it becomes a suggestion instead of being attached.</p>
          </div>
        </Section>
      )}

      {tableOnly.length > 0 && (
        <InfoLine>
          {tableOnly.map((r) => r.name).join(", ")} {tableOnly.length === 1 ? "checks" : "check"} whole tables — no
          column mapping needed.
        </InfoLine>
      )}
    </div>
  );
}

// ── Step 4: Review ──────────────────────────────────────────────────────

function Stat({ label, value, tone }: { label: string; value: number; tone?: "warn" | "muted" }) {
  return (
    <div className="rounded-lg border bg-card px-3 py-2.5">
      <div
        className={cn(
          "text-xl font-semibold tabular-nums",
          tone === "warn" && value > 0 && "text-amber-600 dark:text-amber-400",
          tone === "muted" && "text-muted-foreground",
        )}
      >
        {value}
      </div>
      <div className="text-[11px] leading-tight text-muted-foreground">{label}</div>
    </div>
  );
}

function AttachmentStatus({ a, suggest, skipped }: { a: PreviewAttachment; suggest: boolean; skipped: boolean }) {
  if (skipped) return <Badge variant="outline" className="text-muted-foreground">Skipped</Badge>;
  if (a.status === "partial") return <Badge variant="outline" className="text-muted-foreground">Not attached</Badge>;
  if (a.status === "ambiguous" || suggest)
    return (
      <Badge variant="outline" className="border-amber-500/40 text-amber-700 dark:text-amber-300">
        {a.status === "ambiguous" ? "Suggest · ambiguous" : "Suggest"}
      </Badge>
    );
  return (
    <Badge variant="outline" className="border-emerald-500/40 text-emerald-700 dark:text-emerald-300">
      Attach
    </Badge>
  );
}

function PreviewRow({ table, suggest }: { table: PreviewTable; suggest: boolean }) {
  const [open, setOpen] = useState(false);
  const skipped = !!table.skipped_reason;
  const counted = table.attachments.filter((a) => a.status !== "partial").length;
  return (
    <li>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left hover:bg-muted/40"
        aria-expanded={open}
      >
        {open ? <ChevronDown className="h-4 w-4 shrink-0" /> : <ChevronRight className="h-4 w-4 shrink-0" />}
        <span className={cn("min-w-0 flex-1 truncate font-mono text-xs", skipped && "text-muted-foreground")}>
          {table.table_fqn}
        </span>
        {skipped ? (
          <Badge variant="outline" className="text-muted-foreground">
            {table.skipped_reason}
          </Badge>
        ) : !table.monitored ? (
          <Badge variant="outline" className="border-primary/40 text-primary">
            Will be registered
          </Badge>
        ) : null}
        <span className="w-20 shrink-0 text-right text-xs text-muted-foreground tabular-nums">
          {counted} {counted === 1 ? "match" : "matches"}
        </span>
      </button>
      {open && (
        <div className="border-t bg-muted/20 px-3 py-2 pl-9">
          {skipped && (
            <p className="pb-2 text-xs text-muted-foreground">
              The policy owner needs APPLY or MANAGE on this table. Nothing is attached here.
            </p>
          )}
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-muted-foreground">
                <th className="py-1 pr-2 font-medium">Rule</th>
                <th className="py-1 pr-2 font-medium">Columns</th>
                <th className="py-1 pr-2 font-medium">Status</th>
                <th className="py-1 font-medium">Reason</th>
              </tr>
            </thead>
            <tbody>
              {table.attachments.map((a, i) => {
                const rule = ruleById(a.rule_id);
                const mapping = Object.entries(a.mapping);
                return (
                  <tr key={i} className="border-t border-border/60 align-top">
                    <td className="py-1.5 pr-2">{rule?.name ?? a.rule_id}</td>
                    <td className="py-1.5 pr-2 font-mono">
                      {mapping.length === 0 ? (
                        <span className="text-muted-foreground">whole table</span>
                      ) : (
                        mapping.map(([slot, cols]) => (
                          <div key={slot}>
                            {`{{${slot}}}`} → {cols.join(" | ")}
                          </div>
                        ))
                      )}
                    </td>
                    <td className="py-1.5 pr-2">
                      <AttachmentStatus a={a} suggest={suggest} skipped={skipped} />
                    </td>
                    <td className="py-1.5 text-muted-foreground">{a.reason}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </li>
  );
}

function ReviewItem({ label, children, onEdit }: { label: string; children: ReactNode; onEdit: () => void }) {
  return (
    <div className="grid grid-cols-[9rem_minmax(0,1fr)_auto] items-start gap-3 py-2">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="min-w-0 text-sm">{children}</dd>
      <Button type="button" variant="ghost" size="sm" className="h-6 px-1.5 text-xs" onClick={onEdit} aria-label={`Edit ${label}`}>
        <Pencil className="h-3 w-3" />
      </Button>
    </div>
  );
}

function CondView({ cond }: { cond: TableCondition | ColumnCondition }) {
  if (cond.kind === "tags" && cond.tags.length)
    return (
      <span className="flex flex-wrap gap-1">
        {cond.tags.map((t) => (
          <TagChip key={t} tag={t} />
        ))}
      </span>
    );
  if (cond.kind === "builder") return <code className="break-words font-mono text-xs">{conditionDetail(cond)}</code>;
  return <span>{conditionDetail(cond)}</span>;
}

export function StepReview({ draft, goTo }: { draft: PolicyDraft; goTo: (s: StepKey) => void }) {
  const preview = mockPreview(draft);
  const rules = selectedRules(draft);
  const suggest = draft.action === "suggest";
  const [showAll, setShowAll] = useState(false);
  const visible = showAll ? preview.tables : preview.tables.slice(0, 8);

  return (
    <div className="space-y-8">
      <Section title="Summary">
        <p className="rounded-md border-l-2 border-primary bg-primary/5 px-3 py-2 text-sm">{summarize(draft)}.</p>
        <dl className="divide-y">
          <ReviewItem label="Name" onEdit={() => goTo("rules")}>
            <div className="font-medium">{draft.name}</div>
            {draft.description && <div className="text-xs text-muted-foreground">{draft.description}</div>}
          </ReviewItem>
          <ReviewItem label="Rules" onEdit={() => goTo("rules")}>
            <ul className="space-y-0.5">
              {rules.map((r) => (
                <li key={r.rule_id}>
                  {r.name}
                  {r.status !== "approved" && (
                    <span className="text-xs text-muted-foreground"> · applies once approved</span>
                  )}
                </li>
              ))}
            </ul>
          </ReviewItem>
          <ReviewItem label="Action" onEdit={() => goTo("rules")}>
            {suggest ? "Suggest only" : "Apply automatically"}
          </ReviewItem>
          <ReviewItem label="Scope" onEdit={() => goTo("where")}>
            {scopeText(draft.scope)}
            {draft.register_tables && (
              <div className="text-xs text-muted-foreground">Also starts monitoring matching tables (draft, unscheduled)</div>
            )}
          </ReviewItem>
          <ReviewItem label="Table condition" onEdit={() => goTo("where")}>
            <CondView cond={draft.table_condition} />
          </ReviewItem>
          {hasSlots(draft) && (
            <ReviewItem label="Columns" onEdit={() => goTo("columns")}>
              <div className="space-y-1.5">
                {rules
                  .filter((r) => r.slots.length)
                  .flatMap((r) =>
                    r.slots.map((s) => (
                      <div key={`${r.rule_id}.${s.name}`} className="flex flex-wrap items-center gap-1.5">
                        <span className="text-xs text-muted-foreground">
                          {r.name} · <span className="font-mono">{`{{${s.name}}}`}</span>
                        </span>
                        <CondView cond={columnCondition(draft, r.rule_id, s.name)} />
                      </div>
                    )),
                  )}
                {draft.pair_by_tag && (
                  <div className="text-xs text-muted-foreground">
                    Paired by tag <span className="font-mono">{draft.pair_by_tag}</span>, then by name
                  </div>
                )}
              </div>
            </ReviewItem>
          )}
        </dl>
      </Section>

      <Separator />

      <Section
        title="Preview"
        description="Dry run against current tags. Nothing is attached until you create the policy."
      >
        <div className="grid grid-cols-5 gap-2">
          <Stat label="Monitored tables matched" value={preview.matched_monitored} />
          <Stat label={suggest ? "Suggestions" : "Attachments"} value={preview.attachments} />
          <Stat label="Tables to register" value={preview.to_register} tone={draft.register_tables ? undefined : "muted"} />
          <Stat label="Skipped — no APPLY or MANAGE" value={preview.skipped} tone="warn" />
          <Stat label="Ambiguous — will be suggested" value={preview.ambiguous} tone="warn" />
        </div>
        {preview.tables.length === 0 ? (
          <p className="rounded-md border border-dashed px-4 py-6 text-center text-sm text-muted-foreground">
            No tables match yet.
          </p>
        ) : (
          <div className="rounded-md border">
            <ul className="divide-y">
              {visible.map((t) => (
                <PreviewRow key={t.table_fqn} table={t} suggest={suggest} />
              ))}
            </ul>
            {preview.tables.length > 8 && (
              <button
                type="button"
                onClick={() => setShowAll((s) => !s)}
                className="w-full border-t px-3 py-2 text-xs text-muted-foreground hover:bg-muted/40 hover:text-foreground"
              >
                {showAll ? "Show fewer" : `Show all ${preview.tables.length} tables`}
              </button>
            )}
          </div>
        )}
      </Section>
    </div>
  );
}
