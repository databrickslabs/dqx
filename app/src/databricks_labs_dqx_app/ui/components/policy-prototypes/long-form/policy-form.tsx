// THROWAWAY prototype — variant A policy form: one long ABAC-style page. A policy is 1:1 with a rule.
import { useState } from "react";
import { ArrowLeft, Ban, Lock, Plus, SlidersHorizontal, Tag, Tags, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { MultiSelectPopover, type MultiSelectOption } from "@/components/monitored-tables/MultiSelectPopover";
import {
  ALL,
  MOCK_CATALOGS,
  copyForRule,
  defaultColumnBuilder,
  defaultColumnCondition,
  defaultRow,
  normalizeAll,
  pruneScope,
  ruleById,
  schemasFor,
  tablesFor,
  type ColumnCondition,
  type LfRule,
  type MockSlot,
  type Policy,
  type PolicyScope,
  type TableCondition,
} from "./model";
import { ChoiceTiles, FamilyBadge, FieldError, FormSection, StatusBadge } from "./primitives";
import { ConditionBuilder } from "./condition-builder";
import { TagMultiSelect, TagSingleSelect } from "./tag-picker";
import { RulePickerDialog } from "./rule-picker-dialog";
import { PreviewSection } from "./preview-section";

type Errors = Partial<Record<"name", string>>;

function validate(d: Policy, takenNames: string[]): Errors {
  const e: Errors = {};
  if (!d.name.trim()) e.name = "Name is required.";
  else if (takenNames.includes(d.name.trim().toLowerCase())) e.name = "A policy with this name already exists.";
  return e;
}

// ── Scope: Catalogs → Schemas → Tables multiselects ─────────────────────────
function ScopePicker({ value, onChange }: { value: PolicyScope; onChange: (next: PolicyScope) => void }) {
  const withAll = (label: string, values: string[]): MultiSelectOption[] => [
    { value: ALL, label },
    ...values.map((v) => ({ value: v, label: v })),
  ];
  const catalogOptions = withAll("All catalogs", Object.keys(MOCK_CATALOGS));
  const schemaOptions = withAll("All schemas", schemasFor(value.catalogs));
  const tableOptions = withAll("All tables", tablesFor(value));

  return (
    <div className="grid gap-3 sm:grid-cols-3">
      <MultiSelectPopover
        label="Catalogs"
        placeholder="All catalogs"
        searchPlaceholder="Search catalogs"
        options={catalogOptions}
        selected={value.catalogs}
        onChange={(next) => onChange(pruneScope({ ...value, catalogs: normalizeAll(value.catalogs, next) }))}
        emptyText="No catalogs match"
      />
      <MultiSelectPopover
        label="Schemas"
        placeholder="All schemas"
        searchPlaceholder="Search schemas"
        options={schemaOptions}
        selected={value.schemas}
        onChange={(next) => onChange(pruneScope({ ...value, schemas: normalizeAll(value.schemas, next) }))}
        emptyText="No schemas match"
      />
      <MultiSelectPopover
        label="Tables"
        placeholder="All tables"
        searchPlaceholder="Search tables"
        options={tableOptions}
        selected={value.tables}
        onChange={(next) => onChange({ ...value, tables: normalizeAll(value.tables, next) })}
        emptyText="No tables match"
      />
    </div>
  );
}

// ── Column condition editor for one slot ────────────────────────────────────
function SlotConditionEditor({
  rule,
  slot,
  value,
  onChange,
}: {
  rule: LfRule;
  slot: MockSlot;
  value: ColumnCondition;
  onChange: (next: ColumnCondition) => void;
}) {
  const label = `{{${slot.name}}}`;
  return (
    <div className="space-y-3 rounded-lg border p-4">
      <div className="flex items-center gap-2">
        <span className="font-mono text-sm font-medium">{label}</span>
        <FamilyBadge family={slot.family} />
      </div>
      <ChoiceTiles
        size="sm"
        ariaLabel={`Column condition for ${label}`}
        value={value.kind}
        onChange={(kind) => {
          if (kind === value.kind) return;
          onChange(kind === "tags" ? defaultColumnCondition(rule) : defaultColumnBuilder(rule, slot));
        }}
        options={[
          { value: "tags", label: "Columns matching selected tags", icon: Tags },
          { value: "builder", label: "Custom condition", icon: SlidersHorizontal },
        ]}
      />
      {value.kind === "tags" ? (
        <TagMultiSelect value={value.tags} onChange={(tags) => onChange({ kind: "tags", tags })} />
      ) : (
        <ConditionBuilder
          expr={value.expr}
          onChange={(expr) => onChange({ kind: "builder", expr })}
          level="column"
          slotName={slot.name}
        />
      )}
    </div>
  );
}

// ── Column conditions for one rule: one block per slot, plus pairing for multi-slot rules ──
interface ColumnSettings {
  column_conditions: Policy["column_conditions"];
  pair_by_tag: Policy["pair_by_tag"];
}

function RuleColumnConditions({
  rule,
  value,
  onChange,
}: {
  rule: LfRule;
  value: ColumnSettings;
  onChange: (next: ColumnSettings) => void;
}) {
  if (rule.slots.length === 0) return <p className="text-sm text-muted-foreground">Runs on whole tables.</p>;
  const slotCond = (slot: MockSlot): ColumnCondition => value.column_conditions[slot.name] ?? defaultColumnCondition(rule);
  return (
    <>
      {rule.slots.map((slot) => (
        <SlotConditionEditor
          key={slot.name}
          rule={rule}
          slot={slot}
          value={slotCond(slot)}
          onChange={(c) => onChange({ ...value, column_conditions: { ...value.column_conditions, [slot.name]: c } })}
        />
      ))}
      {rule.slots.length >= 2 && (
        <div className="space-y-2 rounded-lg border bg-muted/20 p-4">
          <div className="text-sm font-medium">Pair columns by tag</div>
          <p className="text-xs text-muted-foreground">Pairs columns that share this tag's value. Defaults to pairing by name.</p>
          <TagSingleSelect
            className="max-w-xs"
            value={value.pair_by_tag ?? ""}
            onChange={(v) => onChange({ ...value, pair_by_tag: v || null })}
            mode="key"
            placeholder="Pair by name (default)"
            clearable
          />
        </div>
      )}
    </>
  );
}

function RuleChip({ rule, current, onRemove }: { rule: LfRule; current?: boolean; onRemove?: () => void }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-md border bg-card py-1 pr-1 pl-2.5 text-sm">
      <span className="font-medium">{rule.name}</span>
      {current && <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">this rule</span>}
      {rule.status !== "approved" && <StatusBadge status={rule.status} />}
      {onRemove ? (
        <button
          type="button"
          aria-label={`Remove ${rule.name}`}
          onClick={onRemove}
          className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      ) : (
        <span className="w-1" aria-hidden />
      )}
    </span>
  );
}

// ── Main form ───────────────────────────────────────────────────────────────
export function PolicyForm({
  initial,
  isEdit,
  readOnly,
  takenNames,
  onCancel,
  onSave,
}: {
  initial: Policy;
  isEdit: boolean;
  /** No MODIFY on the rule: every field is disabled and there is no save. */
  readOnly?: boolean;
  takenNames: string[];
  onCancel: () => void;
  /** *copies* = new, independent policies for the other rules added in the Rules section. */
  onSave: (policy: Policy, copies: Policy[]) => void;
}) {
  const [draft, setDraft] = useState<Policy>(initial);
  /** Other rules, each with its own column settings; shared fields come from *draft* at save time. */
  const [extras, setExtras] = useState<({ rule_id: string } & ColumnSettings)[]>([]);
  const [submitted, setSubmitted] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);

  const rule = ruleById(draft.rule_id);
  const errors = submitted ? validate(draft, takenNames) : {};
  const set = <K extends keyof Policy>(key: K, value: Policy[K]) => setDraft((d) => ({ ...d, [key]: value }));

  const copies: Policy[] = extras.map((e) => ({
    ...copyForRule(draft, e.rule_id),
    column_conditions: e.column_conditions,
    pair_by_tag: e.pair_by_tag,
  }));

  const addRules = (ids: string[]) =>
    setExtras((prev) => [
      ...prev,
      ...ids.map((rid) => {
        const c = copyForRule(draft, rid);
        return { rule_id: rid, column_conditions: c.column_conditions, pair_by_tag: c.pair_by_tag };
      }),
    ]);
  const setExtra = (rid: string, next: ColumnSettings) =>
    setExtras((prev) => prev.map((e) => (e.rule_id === rid ? { rule_id: rid, ...next } : e)));

  const setTableKind = (kind: TableCondition["kind"]) => {
    if (kind === draft.table_condition.kind) return;
    set(
      "table_condition",
      kind === "none" ? { kind } : kind === "tags" ? { kind, tags: [] } : { kind, expr: { rows: [defaultRow("table")] } },
    );
  };

  const submit = () => {
    setSubmitted(true);
    if (validate(draft, takenNames).name) {
      document.getElementById("policy-section-name")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    const name = draft.name.trim();
    onSave({ ...draft, name }, copies.map((c) => ({ ...c, name })));
  };

  const n = extras.length;
  const saveLabel = isEdit
    ? n === 0
      ? "Save"
      : `Save and create ${n} ${n === 1 ? "copy" : "copies"}`
    : n === 0
      ? "Create policy"
      : `Create ${n + 1} policies`;

  const ruleGroups = [
    { rule, value: { column_conditions: draft.column_conditions, pair_by_tag: draft.pair_by_tag }, onChange: (v: ColumnSettings) => setDraft((d) => ({ ...d, ...v })) },
    ...extras.map((e) => ({
      rule: ruleById(e.rule_id),
      value: { column_conditions: e.column_conditions, pair_by_tag: e.pair_by_tag },
      onChange: (v: ColumnSettings) => setExtra(e.rule_id, v),
    })),
  ].filter((g): g is { rule: LfRule; value: ColumnSettings; onChange: (v: ColumnSettings) => void } => !!g.rule);

  return (
    <div className="pb-4">
      <button
        type="button"
        onClick={onCancel}
        className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" /> Back to policies
      </button>
      <div className="mt-3 mb-2">
        <h2 className="text-xl font-semibold">{isEdit ? initial.name : "New policy"}</h2>
        <p className="text-sm text-muted-foreground">
          Attach <span className="font-medium text-foreground">{rule?.name}</span> to every table and column that matches.
        </p>
      </div>
      {readOnly && (
        <div className="mb-2 flex items-center gap-2 rounded-md border bg-muted/40 px-3 py-2 text-sm text-muted-foreground">
          <Lock className="h-4 w-4 shrink-0" />
          You need Modify on this rule to change its policies
        </div>
      )}
      <Separator />

      <fieldset disabled={readOnly} className="min-w-0">
        {/* 1. Identification */}
        <FormSection id="policy-section-name" title="Policy identification" description="Name the policy so stewards can find it.">
          <div className="space-y-1.5">
            <label htmlFor="policy-name" className="text-sm font-medium">
              Name<span className="text-destructive">*</span>
            </label>
            <Input
              id="policy-name"
              value={draft.name}
              onChange={(e) => set("name", e.target.value)}
              placeholder="e.g. PII emails in prod"
              aria-invalid={!!errors.name}
            />
            <FieldError message={errors.name} />
          </div>
          <div className="space-y-1.5">
            <label htmlFor="policy-description" className="text-sm font-medium">
              Description
            </label>
            <Textarea
              id="policy-description"
              value={draft.description}
              onChange={(e) => set("description", e.target.value)}
              placeholder="What this policy enforces and why"
              rows={3}
            />
          </div>
        </FormSection>
        <Separator />

        {/* 2. Rules */}
        <FormSection title="Rules" description="Each rule gets its own copy of this policy.">
          <div className="flex flex-wrap items-center gap-2">
            {rule && <RuleChip rule={rule} current />}
            {extras.map((e) => {
              const r = ruleById(e.rule_id);
              return r ? (
                <RuleChip
                  key={r.rule_id}
                  rule={r}
                  onRemove={() => setExtras((prev) => prev.filter((x) => x.rule_id !== r.rule_id))}
                />
              ) : null;
            })}
          </div>
          <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setPickerOpen(true)}>
            <Plus className="h-4 w-4" /> Add rules
          </Button>
          <RulePickerDialog
            open={pickerOpen}
            onOpenChange={setPickerOpen}
            selectedRuleIds={extras.map((e) => e.rule_id)}
            excludeRuleId={draft.rule_id}
            onAdd={addRules}
          />
        </FormSection>
        <Separator />

        {/* 3. Scope */}
        <FormSection
          title="Scope"
          description="Where the policy looks. Only tables you can apply rules to are changed."
        >
          <ScopePicker value={draft.scope} onChange={(scope) => set("scope", scope)} />
          <label className="flex cursor-pointer items-start gap-2.5 rounded-md border bg-card p-3">
            <Checkbox
              checked={draft.register_tables}
              onCheckedChange={(v) => set("register_tables", v === true)}
              className="mt-0.5"
              aria-label="Match against tables not yet monitored in DQX Studio"
            />
            <span className="space-y-0.5">
              <span className="block text-sm font-medium">Match against tables not yet monitored in DQX Studio</span>
              <span className="block text-xs text-muted-foreground">Matching tables are added as draft and unscheduled.</span>
            </span>
          </label>
        </FormSection>
        <Separator />

        {/* 4. Table conditions */}
        <FormSection title="Table conditions" description="Optionally narrow which tables in scope the policy applies to.">
          <ChoiceTiles
            ariaLabel="Table condition"
            value={draft.table_condition.kind}
            onChange={setTableKind}
            options={[
              { value: "none", label: "No condition", icon: Ban, info: "Every table in scope is considered." },
              { value: "tags", label: "Tables matching selected tags", icon: Tag },
              { value: "builder", label: "Custom condition", icon: SlidersHorizontal },
            ]}
          />
          {draft.table_condition.kind === "tags" && (
            <TagMultiSelect value={draft.table_condition.tags} onChange={(tags) => set("table_condition", { kind: "tags", tags })} />
          )}
          {draft.table_condition.kind === "builder" && (
            <ConditionBuilder
              expr={draft.table_condition.expr}
              onChange={(expr) => set("table_condition", { kind: "builder", expr })}
              level="table"
            />
          )}
        </FormSection>
        <Separator />

        {/* 5. Column conditions (one group per rule when there are several) */}
        <FormSection title="Column conditions" description="Which columns fill each of the rule's inputs.">
          {ruleGroups.length === 1 ? (
            <RuleColumnConditions rule={ruleGroups[0].rule} value={ruleGroups[0].value} onChange={ruleGroups[0].onChange} />
          ) : (
            ruleGroups.map((g) => (
              <div key={g.rule.rule_id} className="space-y-3">
                <div className="text-sm font-semibold">{g.rule.name}</div>
                <RuleColumnConditions rule={g.rule} value={g.value} onChange={g.onChange} />
              </div>
            ))
          )}
        </FormSection>
        <Separator />

        {/* 6. Preview */}
        <FormSection title="Preview" description="Tables and columns this policy would match.">
          <PreviewSection policies={[draft, ...copies]} />
        </FormSection>
      </fieldset>

      {/* Footer */}
      <div className="sticky bottom-0 z-10 -mx-6 flex items-center gap-2 border-t bg-background/95 px-6 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        {readOnly ? (
          <Button variant="outline" onClick={onCancel}>
            Close
          </Button>
        ) : (
          <>
            <Button onClick={submit}>{saveLabel}</Button>
            <Button variant="outline" onClick={onCancel}>
              Cancel
            </Button>
            {submitted && Object.keys(errors).length > 0 && (
              <span className="ml-2 text-xs font-medium text-destructive">Fix the highlighted fields to continue.</span>
            )}
          </>
        )}
      </div>
    </div>
  );
}
