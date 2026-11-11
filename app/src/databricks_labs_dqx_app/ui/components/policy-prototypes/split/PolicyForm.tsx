// THROWAWAY prototype — left-hand policy form for variant C. Compact ABAC
// style: each section stacks a bold title + short description above its fields.
import { useState } from "react";
import {
  Ban,
  Columns3,
  Lightbulb,
  ListFilter,
  Plus,
  Sparkles,
  Tags,
  Trash2,
  Wand2,
  X,
  Zap,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectSeparator, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import {
  COLUMN_SUBJECTS,
  MOCK_CATALOGS,
  TABLE_SUBJECTS,
  type ColumnCondition,
  type MockRule,
  type MockSlot,
  type PolicyDraft,
  type TableCondition,
} from "../mock";
import { ConditionBuilder } from "./ConditionBuilder";
import { RulePickerDialog } from "./RulePickerDialog";
import {
  ChoiceTiles,
  FieldError,
  FieldLabel,
  Pill,
  Section,
  cardinalityLabel,
  columnConditionComplete,
  ruleById,
  scopeSummary,
  slotCondition,
  tableConditionComplete,
  type SectionState,
} from "./shared";
import { TagChip, TagMultiPicker, TagSinglePicker } from "./TagPickers";

export interface FormErrors {
  name?: string;
  rules?: string;
  scope?: string;
}

type Update = (fn: (d: PolicyDraft) => PolicyDraft) => void;

const COLUMN_TILES = [
  { value: "tags" as const, label: "Columns matching selected tags", icon: Tags, info: "Columns with any of the selected governed tags. A bare key matches every value of that key." },
  { value: "builder" as const, label: "Columns matching custom condition", icon: Wand2, info: "Combine column, table, schema and catalog properties with AND / OR." },
];

function ColumnConditionEditor({ value, onChange }: { value: ColumnCondition; onChange: (c: ColumnCondition) => void }) {
  return (
    <div className="space-y-2">
      <ChoiceTiles
        size="sm"
        options={COLUMN_TILES}
        value={value.kind}
        onChange={(k) =>
          onChange(
            k === "tags"
              ? { kind: "tags", tags: value.kind === "tags" ? value.tags : [] }
              : { kind: "builder", expr: { rows: [{ combinator: null, subject: "column_tag", operator: "has_tag", value: "" }] } },
          )
        }
      />
      {value.kind === "tags" ? (
        <TagMultiPicker value={value.tags} onChange={(tags) => onChange({ kind: "tags", tags })} invalid={value.tags.length === 0} />
      ) : (
        <ConditionBuilder expr={value.expr} onChange={(expr) => onChange({ kind: "builder", expr })} subjects={COLUMN_SUBJECTS} />
      )}
    </div>
  );
}

function SlotHeader({ slot, rule }: { slot: MockSlot; rule?: MockRule }) {
  return (
    <div className="flex flex-wrap items-center gap-1.5 text-xs">
      <span className="font-mono font-medium">{`{{${slot.name}}}`}</span>
      <span className="text-muted-foreground">·</span>
      <span className="text-muted-foreground">{slot.family === "any" ? "any type" : slot.family}</span>
      <span className="text-muted-foreground">·</span>
      <span className="text-muted-foreground">{cardinalityLabel(slot)}</span>
      {rule && <span className="ml-auto truncate text-[11px] text-muted-foreground">{rule.name}</span>}
    </div>
  );
}

export function PolicyForm({ draft, update, errors }: { draft: PolicyDraft; update: Update; errors: FormErrors }) {
  const [pickerOpen, setPickerOpen] = useState(false);
  const rules = draft.rule_ids.map(ruleById).filter((r): r is MockRule => !!r);
  const slotted = rules.filter((r) => r.slots.length > 0);
  const singleSlot = slotted.filter((r) => r.slots.length === 1);
  const multiSlot = slotted.some((r) => r.slots.length >= 2);
  const [sameForAll, setSameForAll] = useState(false);
  const shareable = sameForAll && singleSlot.length >= 2;

  const setSlot = (ruleId: string, slot: string, c: ColumnCondition) =>
    update((d) => ({ ...d, column_conditions: { ...d.column_conditions, [ruleId]: { ...d.column_conditions[ruleId], [slot]: c } } }));
  const setShared = (c: ColumnCondition) =>
    update((d) => {
      const cc = { ...d.column_conditions };
      for (const r of singleSlot) cc[r.rule_id] = { ...cc[r.rule_id], [r.slots[0].name]: c };
      return { ...d, column_conditions: cc };
    });

  const removeRule = (id: string) =>
    update((d) => {
      const { [id]: _drop, ...cc } = d.column_conditions;
      return { ...d, rule_ids: d.rule_ids.filter((r) => r !== id), column_conditions: cc };
    });

  // Section states
  const idState: SectionState = errors.name ? "error" : draft.name.trim() ? "complete" : "incomplete";
  const rulesState: SectionState = errors.rules ? "error" : rules.length ? "complete" : "incomplete";
  const scopeState: SectionState = errors.scope ? "error" : draft.scope.some((s) => s.catalog) ? "complete" : "incomplete";
  const tableState: SectionState = draft.table_condition.kind === "none" ? "optional" : tableConditionComplete(draft.table_condition) ? "complete" : "incomplete";
  const slotsAll = slotted.flatMap((r) => r.slots.map((s) => slotCondition(draft, r.rule_id, s.name)));
  const colState: SectionState = slotsAll.every(columnConditionComplete) ? "complete" : "incomplete";
  const tc = draft.table_condition;

  return (
    <div className="space-y-3">
      {/* 1. Identification */}
      <Section title="Identification" description="Name the policy so stewards can find it from any rule it covers." state={idState} summary={draft.name || "Unnamed"}>
        <div className="space-y-1">
          <FieldLabel required htmlFor="policy-name">
            Name
          </FieldLabel>
          <Input
            id="policy-name"
            value={draft.name}
            onChange={(e) => update((d) => ({ ...d, name: e.target.value }))}
            placeholder="e.g. PII emails in prod"
            aria-invalid={!!errors.name || undefined}
            className="h-9"
          />
          <FieldError>{errors.name}</FieldError>
        </div>
        <div className="space-y-1">
          <FieldLabel htmlFor="policy-desc">Description</FieldLabel>
          <Textarea
            id="policy-desc"
            value={draft.description}
            onChange={(e) => update((d) => ({ ...d, description: e.target.value }))}
            placeholder="What this policy covers and why"
            rows={2}
            className="min-h-0 text-sm"
          />
        </div>
      </Section>

      {/* 2. Rules */}
      <Section
        title="Rules"
        description="Rules this policy attaches. Only approved rules are ever attached."
        state={rulesState}
        summary={rules.map((r) => r.name).join(", ") || "None"}
      >
        <div className="flex flex-wrap gap-1.5">
          {rules.map((r) => (
            <span key={r.rule_id} className="inline-flex items-center gap-1.5 rounded-md border bg-background py-1 pl-2 pr-1 text-xs dark:bg-input/30">
              <span className="font-medium">{r.name}</span>
              {r.slots.length > 0 && (
                <span className="font-mono text-[10px] text-muted-foreground">{r.slots.map((s) => `{{${s.name}}}`).join(" ")}</span>
              )}
              {r.status !== "approved" && <Pill tone="amber">Applies once approved</Pill>}
              <button
                type="button"
                onClick={() => removeRule(r.rule_id)}
                aria-label={`Remove ${r.name}`}
                className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
              >
                <X className="h-3 w-3" />
              </button>
            </span>
          ))}
          <Button type="button" variant="outline" size="sm" className="h-7 gap-1 text-xs" onClick={() => setPickerOpen(true)}>
            <Plus className="h-3.5 w-3.5" /> Add rules
          </Button>
        </div>
        <FieldError>{errors.rules}</FieldError>
        <RulePickerDialog
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          existing={draft.rule_ids}
          onAdd={(ids) => update((d) => ({ ...d, rule_ids: [...d.rule_ids, ...ids.filter((i) => !d.rule_ids.includes(i))] }))}
        />
      </Section>

      {/* 3. Action */}
      <Section
        title="Action"
        description="What happens when a table or column matches."
        state="complete"
        summary={draft.action === "auto_apply" ? "Apply automatically" : "Suggest only"}
      >
        <ChoiceTiles
          options={[
            { value: "auto_apply", label: "Apply automatically", icon: Zap, info: "Unambiguous matches are attached right away and show “via policy” on the table." },
            { value: "suggest", label: "Suggest only", icon: Lightbulb, info: "Matches appear as suggestions on the table; a steward accepts each one." },
          ]}
          value={draft.action}
          onChange={(action) => update((d) => ({ ...d, action }))}
        />
      </Section>

      {/* 4. Scope */}
      <Section title="Scope" description="Catalogs and schemas the policy looks at." state={scopeState} summary={scopeSummary(draft.scope)}>
        <div className="space-y-1.5">
          <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_32px] gap-1.5 text-[11px] font-medium text-muted-foreground">
            <span>
              Catalog<span className="text-destructive">*</span>
            </span>
            <span>Schema</span>
          </div>
          {draft.scope.map((s, i) => (
            <div key={i} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_32px] items-center gap-1.5">
              <Select
                value={s.catalog || undefined}
                onValueChange={(catalog) => update((d) => ({ ...d, scope: d.scope.map((x, j) => (j === i ? { catalog, schema: "*" } : x)) }))}
              >
                <SelectTrigger size="sm" className="w-full font-mono text-xs" aria-invalid={(!!errors.scope && !s.catalog) || undefined}>
                  <SelectValue placeholder="Select catalog" />
                </SelectTrigger>
                <SelectContent>
                  {Object.keys(MOCK_CATALOGS).map((c) => (
                    <SelectItem key={c} value={c} className="font-mono text-xs">
                      {c}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select
                value={s.schema}
                disabled={!s.catalog}
                onValueChange={(schema) => update((d) => ({ ...d, scope: d.scope.map((x, j) => (j === i ? { ...x, schema } : x)) }))}
              >
                <SelectTrigger size="sm" className="w-full font-mono text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="*" className="text-xs">
                    All schemas
                  </SelectItem>
                  <SelectSeparator />
                  {(MOCK_CATALOGS[s.catalog] ?? []).map((sc) => (
                    <SelectItem key={sc} value={sc} className="font-mono text-xs">
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
                onClick={() => update((d) => ({ ...d, scope: d.scope.filter((_, j) => j !== i) }))}
                aria-label="Remove scope row"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </div>
          ))}
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-7 gap-1 px-2 text-xs"
            onClick={() => update((d) => ({ ...d, scope: [...d.scope, { catalog: "", schema: "*" }] }))}
          >
            <Plus className="h-3.5 w-3.5" /> Add catalog or schema
          </Button>
          <FieldError>{errors.scope}</FieldError>
        </div>
        <label className="flex cursor-pointer items-start gap-2.5 rounded-md border bg-background/50 p-2.5">
          <Checkbox
            checked={draft.register_tables}
            onCheckedChange={(v) => update((d) => ({ ...d, register_tables: v }))}
            className="mt-0.5"
          />
          <span className="space-y-0.5">
            <span className="block text-xs font-medium">Also start monitoring matching tables</span>
            <span className="block text-[11px] text-muted-foreground">Added as draft and unscheduled. Owned by the policy owner.</span>
          </span>
        </label>
      </Section>

      {/* 5. Table condition */}
      <Section
        title="Table condition"
        description="Optionally narrow the scope to tables with certain properties."
        state={tableState}
        summary={tc.kind === "none" ? "No condition" : tc.kind === "tags" ? `Tagged ${tc.tags.join(", ") || "…"}` : "Custom condition"}
        defaultOpen={tc.kind !== "none"}
      >
        <ChoiceTiles<TableCondition["kind"]>
          options={[
            { value: "none", label: "No condition", icon: Ban, info: "Every table in scope is considered." },
            { value: "tags", label: "Tables matching selected tags", icon: Tags, info: "Tables with any of the selected tags, set on the table itself." },
            { value: "builder", label: "Tables matching custom condition", icon: ListFilter, info: "Combine catalog, schema and table tags, owner and name with AND / OR." },
          ]}
          value={tc.kind}
          onChange={(kind) =>
            update((d) => ({
              ...d,
              table_condition:
                kind === "none"
                  ? { kind }
                  : kind === "tags"
                    ? { kind, tags: d.table_condition.kind === "tags" ? d.table_condition.tags : [] }
                    : { kind, expr: { rows: [{ combinator: null, subject: "table_tag", operator: "has_tag_value", value: "" }] } },
            }))
          }
        />
        {tc.kind === "tags" && (
          <TagMultiPicker
            value={tc.tags}
            onChange={(tags) => update((d) => ({ ...d, table_condition: { kind: "tags", tags } }))}
            invalid={tc.tags.length === 0}
          />
        )}
        {tc.kind === "builder" && (
          <ConditionBuilder
            expr={tc.expr}
            onChange={(expr) => update((d) => ({ ...d, table_condition: { kind: "builder", expr } }))}
            subjects={TABLE_SUBJECTS}
          />
        )}
      </Section>

      {/* 6. Column conditions */}
      {slotted.length > 0 && (
        <Section
          title="Column conditions"
          description="For each rule input, which columns it binds to. Only type-compatible columns are considered."
          state={colState}
          summary={`${slotsAll.length} input${slotsAll.length === 1 ? "" : "s"}`}
        >
          {singleSlot.length >= 2 && (
            <label className="flex cursor-pointer items-center justify-between gap-3 rounded-md border bg-background/50 px-3 py-2">
              <span>
                <span className="block text-xs font-medium">Use the same condition for all rules</span>
                <span className="block text-[11px] text-muted-foreground">
                  Applies to the {singleSlot.length} single-column rules{multiSlot ? ". Multi-column rules keep their own." : "."}
                </span>
              </span>
              <Switch
                checked={sameForAll}
                onCheckedChange={(on) => {
                  setSameForAll(on);
                  if (on) setShared(slotCondition(draft, singleSlot[0].rule_id, singleSlot[0].slots[0].name));
                }}
              />
            </label>
          )}

          {shareable && (
            <div className="space-y-2 rounded-md border p-3">
              <div className="flex flex-wrap items-center gap-1.5 text-xs">
                <Sparkles className="h-3.5 w-3.5 text-primary" />
                <span className="font-medium">Shared condition</span>
                <span className="text-muted-foreground">
                  for {singleSlot.map((r) => `{{${r.slots[0].name}}}`).join(", ")}
                </span>
              </div>
              <ColumnConditionEditor value={slotCondition(draft, singleSlot[0].rule_id, singleSlot[0].slots[0].name)} onChange={setShared} />
            </div>
          )}

          {slotted
            .filter((r) => !(shareable && r.slots.length === 1))
            .map((r) => (
              <div key={r.rule_id} className="space-y-2 rounded-md border p-3">
                <div className="flex items-center gap-1.5">
                  <Columns3 className="h-3.5 w-3.5 text-muted-foreground" />
                  <span className="text-xs font-semibold">{r.name}</span>
                </div>
                {r.slots.map((s) => (
                  <div key={s.name} className={cn("space-y-1.5", r.slots.length > 1 && "border-l-2 pl-3")}>
                    <SlotHeader slot={s} />
                    <ColumnConditionEditor value={slotCondition(draft, r.rule_id, s.name)} onChange={(c) => setSlot(r.rule_id, s.name, c)} />
                  </div>
                ))}
              </div>
            ))}

          {multiSlot && (
            <div className="space-y-1.5 rounded-md border border-dashed p-3">
              <div className="text-xs font-semibold">Pair columns by tag</div>
              <p className="text-[11px] leading-relaxed text-muted-foreground">
                For rules with several inputs, columns are paired when they share a value for this tag key (e.g.{" "}
                <span className="font-mono">pair=order</span>). Without it, columns are paired by name, so{" "}
                <span className="font-mono">order_start_date</span> goes with <span className="font-mono">order_end_date</span>. Ties are
                sent as suggestions.
              </p>
              <div className="flex items-center gap-1.5">
                {draft.pair_by_tag ? (
                  <TagChip tag={draft.pair_by_tag} onRemove={() => update((d) => ({ ...d, pair_by_tag: null }))} />
                ) : (
                  <div className="w-48">
                    <TagSinglePicker
                      value=""
                      mode="keys"
                      placeholder="Optional tag key"
                      onChange={(k) => update((d) => ({ ...d, pair_by_tag: k }))}
                    />
                  </div>
                )}
              </div>
            </div>
          )}
        </Section>
      )}
    </div>
  );
}
