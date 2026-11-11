// THROWAWAY prototype — variant A policy form: one long ABAC-style page.
import { useState } from "react";
import {
  ArrowLeft,
  Ban,
  Bell,
  Columns3,
  Plus,
  SlidersHorizontal,
  Tag,
  Tags,
  WandSparkles,
  X,
} from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import {
  COLUMN_SUBJECTS,
  CURRENT_USER,
  MOCK_CATALOGS,
  TABLE_SUBJECTS,
  type ColumnCondition,
  type MockRule,
  type MockSlot,
  type PolicyDraft,
  type TableCondition,
} from "../mock";
import { ChoiceTiles, FieldError, FormSection, StatusBadge, ruleById, slotSummary } from "./primitives";
import { ConditionBuilder, defaultRow } from "./condition-builder";
import { TagMultiSelect, TagSingleSelect } from "./tag-picker";
import { RulePickerDialog } from "./rule-picker-dialog";
import { PreviewSection } from "./preview-section";

type Errors = Partial<Record<"name" | "rules" | "scope", string>>;

function validate(d: PolicyDraft, takenNames: string[]): Errors {
  const e: Errors = {};
  if (!d.name.trim()) e.name = "Name is required.";
  else if (takenNames.includes(d.name.trim().toLowerCase())) e.name = "A policy with this name already exists.";
  if (d.rule_ids.length === 0) e.rules = "Add at least one rule.";
  if (!d.scope.some((s) => s.catalog)) e.scope = "Select at least one catalog.";
  return e;
}

const DEFAULT_COLUMN_CONDITION: ColumnCondition = { kind: "tags", tags: [] };

// ── Column condition editor for one slot ────────────────────────────────────
function SlotConditionEditor({
  label,
  slot,
  value,
  onChange,
}: {
  label: string;
  slot: MockSlot;
  value: ColumnCondition;
  onChange: (next: ColumnCondition) => void;
}) {
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-x-2 text-sm">
        <span className="font-mono font-medium">{label}</span>
        <span className="text-xs text-muted-foreground">{slotSummary(slot)}</span>
      </div>
      <ChoiceTiles
        size="sm"
        ariaLabel={`Column condition for ${label}`}
        value={value.kind}
        onChange={(kind) =>
          onChange(kind === "tags" ? { kind: "tags", tags: [] } : { kind: "builder", expr: { rows: [defaultRow("column_tag")] } })
        }
        options={[
          {
            value: "tags",
            label: "Columns matching selected tags",
            icon: Tags,
            info: "A column matches if it has any of the selected tags. A bare key matches any value.",
          },
          {
            value: "builder",
            label: "Columns matching custom condition",
            icon: SlidersHorizontal,
            info: "Combine column, table, schema and catalog properties with AND / OR.",
          },
        ]}
      />
      {value.kind === "tags" ? (
        <TagMultiSelect value={value.tags} onChange={(tags) => onChange({ kind: "tags", tags })} />
      ) : (
        <ConditionBuilder expr={value.expr} onChange={(expr) => onChange({ kind: "builder", expr })} subjects={COLUMN_SUBJECTS} />
      )}
      {slot.family !== "any" && (
        <p className="text-xs text-muted-foreground">Only {slot.family} columns are considered.</p>
      )}
    </div>
  );
}

// ── Main form ───────────────────────────────────────────────────────────────
export function PolicyForm({
  initial,
  isEdit,
  takenNames,
  onCancel,
  onSave,
}: {
  initial: PolicyDraft;
  isEdit: boolean;
  takenNames: string[];
  onCancel: () => void;
  onSave: (draft: PolicyDraft) => void;
}) {
  const [draft, setDraft] = useState<PolicyDraft>(initial);
  const [submitted, setSubmitted] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [shared, setShared] = useState(false);

  const errors = submitted ? validate(draft, takenNames) : {};
  const set = <K extends keyof PolicyDraft>(key: K, value: PolicyDraft[K]) => setDraft((d) => ({ ...d, [key]: value }));

  const rules = draft.rule_ids.map(ruleById).filter((r): r is MockRule => !!r);
  const slotted = rules.filter((r) => r.slots.length > 0);
  const slotsLineUp = slotted.length >= 2 && slotted.every((r) => r.slots.length === slotted[0].slots.length);
  const sharedOn = shared && slotsLineUp;
  const needsPairing = rules.some((r) => r.slots.length >= 2);

  const slotCond = (ruleId: string, slot: string): ColumnCondition =>
    draft.column_conditions[ruleId]?.[slot] ?? DEFAULT_COLUMN_CONDITION;
  const setSlotCond = (ruleId: string, slot: string, c: ColumnCondition) =>
    setDraft((d) => ({
      ...d,
      column_conditions: { ...d.column_conditions, [ruleId]: { ...d.column_conditions[ruleId], [slot]: c } },
    }));
  const setSharedSlot = (index: number, c: ColumnCondition) =>
    setDraft((d) => {
      const next = { ...d.column_conditions };
      for (const r of slotted) next[r.rule_id] = { ...next[r.rule_id], [r.slots[index].name]: c };
      return { ...d, column_conditions: next };
    });

  const removeRule = (id: string) =>
    setDraft((d) => {
      const { [id]: _removed, ...rest } = d.column_conditions;
      void _removed;
      return { ...d, rule_ids: d.rule_ids.filter((r) => r !== id), column_conditions: rest };
    });

  const setScope = (i: number, patch: Partial<PolicyDraft["scope"][number]>) =>
    set(
      "scope",
      draft.scope.map((s, j) => (j === i ? { ...s, ...patch } : s)),
    );

  const setTableKind = (kind: TableCondition["kind"]) => {
    if (kind === draft.table_condition.kind) return;
    set(
      "table_condition",
      kind === "none" ? { kind } : kind === "tags" ? { kind, tags: [] } : { kind, expr: { rows: [defaultRow("table_tag")] } },
    );
  };

  const submit = () => {
    setSubmitted(true);
    const e = validate(draft, takenNames);
    const first = (["name", "rules", "scope"] as const).find((k) => e[k]);
    if (first) {
      document.getElementById(`policy-section-${first}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    onSave({ ...draft, name: draft.name.trim(), owner: draft.owner ?? CURRENT_USER });
  };

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
        <h2 className="text-xl font-semibold">{isEdit ? `Edit policy · ${initial.name}` : "New policy"}</h2>
        <p className="text-sm text-muted-foreground">
          Attach approved rules to every table and column that matches a scope and conditions.
        </p>
      </div>
      <Separator />

      {/* 1. Identification */}
      <FormSection id="policy-section-name" title="Policy identification" description="Name the policy so stewards can find it from any rule it covers.">
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
      <FormSection
        id="policy-section-rules"
        title="Rules"
        required
        description="Rules this policy attaches. Only approved rule versions are attached; drafts apply once approved."
      >
        <div
          className={cn(
            "flex min-h-12 flex-wrap items-center gap-2 rounded-md border bg-background p-2",
            errors.rules && "border-destructive",
          )}
        >
          {rules.length === 0 && <span className="px-1 text-sm text-muted-foreground">No rules selected</span>}
          {rules.map((r) => (
            <span key={r.rule_id} className="inline-flex items-center gap-2 rounded-md border bg-card py-1 pr-1 pl-2.5 text-sm">
              <span className="font-medium">{r.name}</span>
              {r.status !== "approved" && (
                <span className="text-[11px] text-amber-700 dark:text-amber-300">Applies once approved</span>
              )}
              <button
                type="button"
                aria-label={`Remove ${r.name}`}
                onClick={() => removeRule(r.rule_id)}
                className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </span>
          ))}
        </div>
        <FieldError message={errors.rules} />
        <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setPickerOpen(true)}>
          <Plus className="h-4 w-4" /> Add rules
        </Button>
        <RulePickerDialog
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          selectedRuleIds={draft.rule_ids}
          onAdd={(ids) => set("rule_ids", [...draft.rule_ids, ...ids])}
        />
      </FormSection>
      <Separator />

      {/* 3. Action */}
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
              info: "Matching rules are attached straight away and marked “via policy”. Ambiguous matches are still suggested.",
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

      {/* 4. Scope */}
      <FormSection
        id="policy-section-scope"
        title="Scope"
        required
        description="Catalogs and schemas the policy looks in. It only attaches to tables where you hold APPLY or MANAGE."
      >
        <div className="space-y-2">
          <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_36px] gap-2 text-xs font-medium text-muted-foreground">
            <span>Catalog</span>
            <span>Schema</span>
            <span />
          </div>
          {draft.scope.map((s, i) => (
            <div key={i} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_36px] items-center gap-2">
              <Select value={s.catalog || undefined} onValueChange={(catalog) => setScope(i, { catalog, schema: "*" })}>
                <SelectTrigger className={cn("w-full", errors.scope && !s.catalog && "border-destructive")}>
                  <SelectValue placeholder="Select catalog" />
                </SelectTrigger>
                <SelectContent>
                  {Object.keys(MOCK_CATALOGS).map((c) => (
                    <SelectItem key={c} value={c} className="font-mono">
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
                    <SelectItem key={sc} value={sc} className="font-mono">
                      {sc}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {draft.scope.length > 1 ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label="Remove scope row"
                  onClick={() => set("scope", draft.scope.filter((_, j) => j !== i))}
                >
                  <X className="h-4 w-4" />
                </Button>
              ) : (
                <span aria-hidden />
              )}
            </div>
          ))}
          <FieldError message={errors.scope} />
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-1.5"
            onClick={() => set("scope", [...draft.scope, { catalog: "", schema: "*" }])}
          >
            <Plus className="h-4 w-4" /> Add catalog or schema
          </Button>
        </div>
        <label className="flex cursor-pointer items-start gap-2.5 rounded-md border bg-card p-3">
          <Checkbox
            checked={draft.register_tables}
            onCheckedChange={(v) => set("register_tables", v)}
            className="mt-0.5"
            aria-label="Also start monitoring matching tables"
          />
          <span className="space-y-0.5">
            <span className="block text-sm font-medium">Also start monitoring matching tables</span>
            <span className="block text-xs text-muted-foreground">
              Unmonitored tables that match are added as draft and unscheduled, owned by you.
            </span>
          </span>
        </label>
      </FormSection>
      <Separator />

      {/* 5. Table condition */}
      <FormSection title="Table condition" description="Optionally narrow which tables in scope the policy applies to.">
        <ChoiceTiles
          ariaLabel="Table condition"
          value={draft.table_condition.kind}
          onChange={setTableKind}
          options={[
            { value: "none", label: "No condition", icon: Ban, info: "Every table in scope is considered." },
            {
              value: "tags",
              label: "Tables matching selected tags",
              icon: Tag,
              info: "A table matches if it has any of the selected tags. A bare key matches any value.",
            },
            {
              value: "builder",
              label: "Tables matching custom condition",
              icon: SlidersHorizontal,
              info: "Combine table, schema and catalog tags, owner and name with AND / OR.",
            },
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

      {/* 6. Column conditions */}
      {rules.length > 0 && (
        <>
          <FormSection
            title="Column conditions"
            description="Which columns fill each rule's slots. Only columns of a compatible type are considered."
          >
            {slotted.length === 0 ? (
              <p className="rounded-md border border-dashed px-4 py-6 text-center text-sm text-muted-foreground">
                The selected rules run on whole tables, so no column condition is needed.
              </p>
            ) : (
              <>
                {slotsLineUp && (
                  <label className="flex cursor-pointer items-center justify-between gap-4 rounded-md border bg-card p-3">
                    <span className="space-y-0.5">
                      <span className="block text-sm font-medium">Use the same condition for all rules</span>
                      <span className="block text-xs text-muted-foreground">
                        All {slotted.length} rules have {slotted[0].slots.length === 1 ? "one slot" : `${slotted[0].slots.length} slots`}; edit their conditions once.
                      </span>
                    </span>
                    <Switch checked={shared} onCheckedChange={setShared} />
                  </label>
                )}

                {sharedOn ? (
                  <div className="space-y-6 rounded-lg border p-4">
                    <div className="flex flex-wrap items-center gap-2 text-sm font-medium">
                      <Columns3 className="h-4 w-4 text-muted-foreground" />
                      {slotted.map((r) => r.name).join(" · ")}
                    </div>
                    {slotted[0].slots.map((slot, i) => (
                      <SlotConditionEditor
                        key={slot.name}
                        label={slotted.map((r) => `{{${r.slots[i].name}}}`).filter((v, j, a) => a.indexOf(v) === j).join(", ")}
                        slot={slot}
                        value={slotCond(slotted[0].rule_id, slot.name)}
                        onChange={(c) => setSharedSlot(i, c)}
                      />
                    ))}
                  </div>
                ) : (
                  slotted.map((r) => (
                    <div key={r.rule_id} className="space-y-6 rounded-lg border p-4">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium">{r.name}</span>
                        <StatusBadge status={r.status} />
                      </div>
                      {r.slots.map((slot) => (
                        <SlotConditionEditor
                          key={slot.name}
                          label={`{{${slot.name}}}`}
                          slot={slot}
                          value={slotCond(r.rule_id, slot.name)}
                          onChange={(c) => setSlotCond(r.rule_id, slot.name, c)}
                        />
                      ))}
                    </div>
                  ))
                )}

                {rules.some((r) => r.slots.length === 0) && (
                  <p className="text-xs text-muted-foreground">
                    {rules
                      .filter((r) => r.slots.length === 0)
                      .map((r) => r.name)
                      .join(", ")}{" "}
                    {rules.filter((r) => r.slots.length === 0).length === 1 ? "runs" : "run"} on whole tables and needs no
                    column condition.
                  </p>
                )}

                {needsPairing && (
                  <div className="space-y-2 rounded-lg border bg-muted/20 p-4">
                    <div className="text-sm font-medium">Pair columns by tag</div>
                    <p className="text-xs leading-relaxed text-muted-foreground">
                      Rules with several slots need columns paired up, e.g. a start and end date. By default columns are
                      paired by name (<span className="font-mono">order_start_date</span> with{" "}
                      <span className="font-mono">order_end_date</span>). Pick a tag key to pair columns that share its
                      value instead. Pairs that stay unclear are suggested, not attached.
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

      {/* 7. Preview */}
      <FormSection
        title="Preview"
        description="Dry run of this policy against tables you can see. Nothing is attached until you save."
      >
        <PreviewSection draft={draft} />
      </FormSection>

      {/* Footer */}
      <div className="sticky bottom-0 z-10 -mx-6 flex items-center gap-2 border-t bg-background/95 px-6 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        <Button onClick={submit}>{isEdit ? "Save" : "Create policy"}</Button>
        <Button variant="outline" onClick={onCancel}>
          Cancel
        </Button>
        {submitted && Object.keys(errors).length > 0 && (
          <span className="ml-2 text-xs font-medium text-destructive">Fix the highlighted fields to continue.</span>
        )}
      </div>
    </div>
  );
}

export function toastSaved(draft: PolicyDraft, isEdit: boolean, matched: number) {
  toast.success(isEdit ? `Saved "${draft.name}"` : `Created "${draft.name}"`, {
    description: `Evaluating ${matched} matching ${matched === 1 ? "table" : "tables"} in the background.`,
  });
}
