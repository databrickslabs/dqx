// THROWAWAY prototype — variant A policy form: one long ABAC-style page. A policy is 1:1 with a rule.
import { useState } from "react";
import { ArrowLeft, Ban, Bell, Lock, Plus, SlidersHorizontal, Tag, Tags, WandSparkles, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { MultiSelectPopover, type MultiSelectOption } from "@/components/monitored-tables/MultiSelectPopover";
import {
  ALL,
  COLUMN_SUBJECTS,
  MOCK_CATALOGS,
  TABLE_SUBJECTS,
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
          { value: "builder", label: "Columns matching custom condition", icon: SlidersHorizontal },
        ]}
      />
      {value.kind === "tags" ? (
        <TagMultiSelect value={value.tags} onChange={(tags) => onChange({ kind: "tags", tags })} />
      ) : (
        <ConditionBuilder
          expr={value.expr}
          onChange={(expr) => onChange({ kind: "builder", expr })}
          subjects={COLUMN_SUBJECTS}
          slotName={slot.name}
        />
      )}
    </div>
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
  /** *alsoFor* = extra rule ids that each get an independent copy (create only). */
  onSave: (policy: Policy, alsoFor: string[]) => void;
}) {
  const [draft, setDraft] = useState<Policy>(initial);
  const [alsoFor, setAlsoFor] = useState<string[]>([]);
  const [submitted, setSubmitted] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);

  const rule = ruleById(draft.rule_id);
  const errors = submitted ? validate(draft, takenNames) : {};
  const set = <K extends keyof Policy>(key: K, value: Policy[K]) => setDraft((d) => ({ ...d, [key]: value }));

  const slotCond = (slot: MockSlot): ColumnCondition =>
    draft.column_conditions[slot.name] ?? (rule ? defaultColumnCondition(rule) : { kind: "tags", tags: [] });
  const setSlotCond = (slot: string, c: ColumnCondition) =>
    setDraft((d) => ({ ...d, column_conditions: { ...d.column_conditions, [slot]: c } }));

  const setTableKind = (kind: TableCondition["kind"]) => {
    if (kind === draft.table_condition.kind) return;
    set(
      "table_condition",
      kind === "none" ? { kind } : kind === "tags" ? { kind, tags: [] } : { kind, expr: { rows: [defaultRow("table_tag")] } },
    );
  };

  const submit = () => {
    setSubmitted(true);
    if (validate(draft, takenNames).name) {
      document.getElementById("policy-section-name")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    onSave({ ...draft, name: draft.name.trim() }, alsoFor);
  };

  const otherRules = alsoFor.map(ruleById).filter((r): r is LfRule => !!r);

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

        {/* 2. Scope */}
        <FormSection
          title="Scope"
          description="Where the policy looks. It only attaches to tables where you hold APPLY or MANAGE."
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

        {/* 3. Table conditions */}
        <FormSection title="Table conditions" description="Optionally narrow which tables in scope the policy applies to.">
          <ChoiceTiles
            ariaLabel="Table condition"
            value={draft.table_condition.kind}
            onChange={setTableKind}
            options={[
              { value: "none", label: "No condition", icon: Ban, info: "Every table in scope is considered." },
              { value: "tags", label: "Tables matching selected tags", icon: Tag },
              { value: "builder", label: "Tables matching custom condition", icon: SlidersHorizontal },
            ]}
          />
          {draft.table_condition.kind === "tags" && (
            <TagMultiSelect value={draft.table_condition.tags} onChange={(tags) => set("table_condition", { kind: "tags", tags })} />
          )}
          {draft.table_condition.kind === "builder" && (
            <ConditionBuilder
              expr={draft.table_condition.expr}
              onChange={(expr) => set("table_condition", { kind: "builder", expr })}
              subjects={TABLE_SUBJECTS}
            />
          )}
        </FormSection>
        <Separator />

        {/* 4. Column conditions */}
        {rule && (
          <>
            <FormSection title="Column conditions" description="Which columns fill each of the rule's inputs.">
              {rule.slots.length === 0 ? (
                <p className="text-sm text-muted-foreground">This rule runs on whole tables.</p>
              ) : (
                <>
                  {rule.slots.map((slot) => (
                    <SlotConditionEditor
                      key={slot.name}
                      rule={rule}
                      slot={slot}
                      value={slotCond(slot)}
                      onChange={(c) => setSlotCond(slot.name, c)}
                    />
                  ))}
                  {rule.slots.length >= 2 && (
                    <div className="space-y-2 rounded-lg border bg-muted/20 p-4">
                      <div className="text-sm font-medium">Pair columns by tag</div>
                      <p className="text-xs text-muted-foreground">
                        Pairs columns that share this tag's value. Defaults to pairing by name.
                      </p>
                      <TagSingleSelect
                        className="max-w-xs"
                        value={draft.pair_by_tag ?? ""}
                        onChange={(v) => set("pair_by_tag", v || null)}
                        mode="key"
                        placeholder="Pair by name (default)"
                        clearable
                      />
                    </div>
                  )}
                </>
              )}
            </FormSection>
            <Separator />
          </>
        )}

        {/* 5. Action */}
        <FormSection title="Action" description="What happens when a table or column matches.">
          <ChoiceTiles
            ariaLabel="Action on match"
            value={draft.action}
            onChange={(v) => set("action", v)}
            options={[
              {
                value: "auto_apply",
                label: "Apply automatically",
                icon: WandSparkles,
                info: "The rule is attached straight away and marked “via policy”. Ambiguous matches are still suggested.",
              },
              {
                value: "suggest",
                label: "Suggest only",
                icon: Bell,
                info: "Matches appear as suggestions on each table. A table owner decides whether to attach them.",
              },
            ]}
          />
        </FormSection>
        <Separator />

        {/* 6. Also create for other rules (create only) */}
        {!isEdit && (
          <>
            <FormSection
              title="Also create for other rules"
              description="Creates a separate, identical policy for each rule. Each can then be edited on its own."
            >
              {otherRules.length > 0 && (
                <div className="flex flex-wrap items-center gap-2">
                  {otherRules.map((r) => (
                    <span
                      key={r.rule_id}
                      className="inline-flex items-center gap-2 rounded-md border bg-card py-1 pr-1 pl-2.5 text-sm"
                    >
                      <span className="font-medium">{r.name}</span>
                      {r.status !== "approved" && <StatusBadge status={r.status} />}
                      <button
                        type="button"
                        aria-label={`Remove ${r.name}`}
                        onClick={() => setAlsoFor((prev) => prev.filter((id) => id !== r.rule_id))}
                        className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </span>
                  ))}
                </div>
              )}
              <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setPickerOpen(true)}>
                <Plus className="h-4 w-4" /> Add rules
              </Button>
              <RulePickerDialog
                open={pickerOpen}
                onOpenChange={setPickerOpen}
                selectedRuleIds={alsoFor}
                excludeRuleId={draft.rule_id}
                onAdd={(ids) => setAlsoFor((prev) => [...prev, ...ids])}
              />
            </FormSection>
            <Separator />
          </>
        )}

        {/* 7. Preview */}
        <FormSection title="Preview" description="Tables and columns this policy would match.">
          <PreviewSection draft={draft} />
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
            <Button onClick={submit}>
              {isEdit
                ? "Save"
                : alsoFor.length > 0
                  ? `Create ${alsoFor.length + 1} policies`
                  : "Create policy"}
            </Button>
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
