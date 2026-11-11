// Policy form: one long ABAC-style page. A policy belongs to one rule; extra rules
// added in the Rules section each get their own independent copy on save.
import { useMemo, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { useQueries, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ArrowLeft, Ban, Loader2, Lock, Plus, SlidersHorizontal, Tag, Tags, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { MultiSelectPopover, type MultiSelectOption } from "@/components/monitored-tables/MultiSelectPopover";
import { FamilyBadge } from "@/components/apply-rules/MappingChips";
import {
  RESERVED_NAME_KEY,
  StatusBadge,
  getTag,
  type LabelColorDefinition,
} from "@/components/RegistryRuleBadges";
import {
  getListRulePoliciesQueryKey,
  getListSchemasQueryOptions,
  getListTablesQueryOptions,
  useCreateRulePolicies,
  useListCatalogs,
  useListRegistryRules,
  useUpdateRulePolicy,
  type RegistryRuleOut,
} from "@/lib/api";
import { extractApiError } from "@/lib/api-error";
import { ChoiceTiles, FieldError, FormSection } from "./PolicyFormPrimitives";
import { ConditionRows, TagMultiSelect } from "./ConditionRows";
import { PolicyPreview, type PreviewTarget } from "./PolicyPreview";
import { PolicyRulesDialog } from "./PolicyRulesDialog";
import {
  ALL,
  columnErrorKey,
  customPrefill,
  defaultColumnCondition,
  defaultColumnConditions,
  defaultRow,
  normalizeAll,
  pruneScope,
  toCreatePayload,
  toUpdatePayload,
  validateDraft,
  type ColumnConditionDraft,
  type ColumnConditionsDraft,
  type ExtraRuleDraft,
  type PolicyDraft,
  type ScopeDraft,
  type TableConditionDraft,
} from "./policy-form-state";

/** Statuses whose rules can carry a policy (drafts attach once approved). */
const POLICY_RULE_STATUSES = new Set(["approved", "draft", "pending_approval"]);

const ruleName = (rule: RegistryRuleOut): string => getTag(rule, RESERVED_NAME_KEY) || rule.rule_id;

const splitScope = (scope: string): [string, string] => {
  const dot = scope.indexOf(".");
  return [scope.slice(0, dot), scope.slice(dot + 1)];
};

/** Options with an "All …" entry first; selected values missing from the list stay visible. */
function withAll(allLabel: string, options: MultiSelectOption[], selected: string[]): MultiSelectOption[] {
  const known = new Set(options.map((o) => o.value));
  const missing = selected.filter((v) => v !== ALL && !known.has(v)).map((v) => ({ value: v, label: v }));
  return [{ value: ALL, label: allLabel }, ...missing, ...options];
}

// ── Scope: Catalogs → Schemas → Tables, cascading from the discovery API ─────
function ScopeFields({
  value,
  onChange,
  disabled,
}: {
  value: ScopeDraft;
  onChange: (next: ScopeDraft) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const explicitCatalogs = value.catalogs.includes(ALL) ? [] : value.catalogs;
  const explicitSchemas = value.schemas.includes(ALL) ? [] : value.schemas;

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs();
  const catalogOptions = useMemo(
    () => (catalogsResp?.data ?? []).map((c) => ({ value: c.name, label: c.name })),
    [catalogsResp],
  );

  const { data: schemaOptions, isLoading: schemasLoading } = useQueries({
    queries: explicitCatalogs.map((catalog) => getListSchemasQueryOptions(catalog)),
    combine: (results) => {
      const opts: MultiSelectOption[] = [];
      explicitCatalogs.forEach((catalog, i) => {
        (results[i]?.data?.data ?? []).forEach((s) => opts.push({ value: `${catalog}.${s.name}`, label: `${catalog}.${s.name}` }));
      });
      return { data: opts, isLoading: results.some((r) => r.isLoading) };
    },
  });

  // Tables come from the picked schemas, else from every schema of the picked catalogs.
  const tableSchemaScopes = explicitSchemas.length > 0 ? explicitSchemas : schemaOptions.map((o) => o.value);
  const { data: tableOptions, isLoading: tablesLoading } = useQueries({
    queries: tableSchemaScopes.map((scope) => {
      const [catalog, schema] = splitScope(scope);
      return getListTablesQueryOptions(catalog, schema);
    }),
    combine: (results) => {
      const opts: MultiSelectOption[] = [];
      tableSchemaScopes.forEach((scope, i) => {
        (results[i]?.data?.data ?? []).forEach((tbl) => opts.push({ value: `${scope}.${tbl.name}`, label: `${scope}.${tbl.name}` }));
      });
      return { data: opts, isLoading: results.some((r) => r.isLoading) };
    },
  });

  const noCatalog = explicitCatalogs.length === 0;
  return (
    <div className="grid gap-3 sm:grid-cols-3">
      <MultiSelectPopover
        label={t("rulePolicies.form.catalogs")}
        placeholder={t("rulePolicies.form.allCatalogs")}
        searchPlaceholder={t("rulePolicies.form.searchCatalogs")}
        options={withAll(t("rulePolicies.form.allCatalogs"), catalogOptions, value.catalogs)}
        selected={value.catalogs}
        onChange={(next) => onChange(pruneScope({ ...value, catalogs: normalizeAll(value.catalogs, next) }))}
        isLoading={catalogsLoading}
        disabled={disabled}
        emptyText={t("rulePolicies.form.emptyCatalogs")}
      />
      <MultiSelectPopover
        label={t("rulePolicies.form.schemas")}
        placeholder={t("rulePolicies.form.allSchemas")}
        searchPlaceholder={t("rulePolicies.form.searchSchemas")}
        options={withAll(t("rulePolicies.form.allSchemas"), schemaOptions, value.schemas)}
        selected={value.schemas}
        onChange={(next) => onChange(pruneScope({ ...value, schemas: normalizeAll(value.schemas, next) }))}
        isLoading={!noCatalog && schemasLoading}
        disabled={disabled || noCatalog}
        disabledHint={t("rulePolicies.form.selectCatalogFirst")}
        emptyText={t("rulePolicies.form.emptySchemas")}
      />
      <MultiSelectPopover
        label={t("rulePolicies.form.tables")}
        placeholder={t("rulePolicies.form.allTables")}
        searchPlaceholder={t("rulePolicies.form.searchTables")}
        options={withAll(t("rulePolicies.form.allTables"), tableOptions, value.tables)}
        selected={value.tables}
        onChange={(next) => onChange({ ...value, tables: normalizeAll(value.tables, next) })}
        isLoading={!noCatalog && (schemasLoading || tablesLoading)}
        disabled={disabled || noCatalog}
        disabledHint={t("rulePolicies.form.selectCatalogFirst")}
        emptyText={t("rulePolicies.form.emptyTables")}
      />
    </div>
  );
}

// ── Column conditions: one card per rule, one divided section per slot ───────
function RuleColumnConditions({
  rule,
  value,
  onChange,
  errors,
  showErrors,
  disabled,
}: {
  rule: RegistryRuleOut;
  value: ColumnConditionsDraft;
  onChange: (next: ColumnConditionsDraft) => void;
  errors: Record<string, string>;
  showErrors: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const slots = rule.definition.slots ?? [];
  const name = ruleName(rule);
  return (
    <div className="overflow-hidden rounded-lg border bg-card">
      <div className="border-b bg-muted/30 px-4 py-2.5 text-sm font-semibold">{name}</div>
      {slots.length === 0 ? (
        <p className="p-4 text-sm text-muted-foreground">{t("rulePolicies.form.wholeTableRule")}</p>
      ) : (
        <div className="divide-y">
          {slots.map((slot) => {
            const label = `{{${slot.name}}}`;
            const cond: ColumnConditionDraft = value[slot.name] ?? defaultColumnCondition(rule, slot.name);
            const set = (next: ColumnConditionDraft) => onChange({ ...value, [slot.name]: next });
            const error = errors[columnErrorKey(rule.rule_id, slot.name)];
            return (
              <div key={slot.name} className="space-y-3 p-4">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-sm font-medium">{label}</span>
                  <FamilyBadge family={slot.family} />
                </div>
                <ChoiceTiles
                  size="xs"
                  disabled={disabled}
                  ariaLabel={t("rulePolicies.form.columnConditionAria", { rule: name, slot: label })}
                  value={cond.kind}
                  onChange={(kind) => {
                    if (kind === cond.kind) return;
                    if (kind === "builder") set(customPrefill(rule, slot.name));
                    else {
                      const fallback = defaultColumnCondition(rule, slot.name);
                      set(fallback.kind === "tags" ? fallback : { kind: "tags", tags: [] });
                    }
                  }}
                  options={[
                    { value: "tags", label: t("rulePolicies.form.tileColumnTags"), icon: Tags },
                    { value: "builder", label: t("rulePolicies.form.tileCustom"), icon: SlidersHorizontal },
                  ]}
                />
                {cond.kind === "tags" ? (
                  <TagMultiSelect
                    value={cond.tags}
                    onChange={(tags) => set({ kind: "tags", tags })}
                    invalid={showErrors && !!error}
                    disabled={disabled}
                  />
                ) : (
                  <ConditionRows
                    rows={cond.rows}
                    onChange={(rows) => set({ kind: "builder", rows })}
                    level="column"
                    slotName={slot.name}
                    showErrors={showErrors}
                    disabled={disabled}
                  />
                )}
                {showErrors && <FieldError message={error ? t(error) : undefined} />}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function RuleChip({ rule, onRemove }: { rule: RegistryRuleOut; onRemove?: () => void }) {
  const { t } = useTranslation();
  const name = ruleName(rule);
  return (
    <span className="inline-flex items-center gap-2 rounded-md border bg-card py-1 pr-1 pl-2.5 text-sm">
      <span className="font-medium">{name}</span>
      {rule.status !== "approved" && <StatusBadge status={rule.status} />}
      {onRemove ? (
        <button
          type="button"
          aria-label={t("rulePolicies.form.removeRule", { name })}
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

interface ExtraRule extends ExtraRuleDraft {
  rule: RegistryRuleOut;
}

export function PolicyForm({
  rule,
  initial,
  readOnly,
  takenNames,
  labelDefinitions,
  onClose,
}: {
  rule: RegistryRuleOut;
  initial: PolicyDraft;
  /** No MODIFY on the rule: every field is disabled and there is no save. */
  readOnly: boolean;
  /** The rule's other policy names (unique per rule). */
  takenNames: string[];
  labelDefinitions: LabelColorDefinition[];
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const isEdit = initial.policyId !== null;
  const [draft, setDraft] = useState<PolicyDraft>(initial);
  const [extras, setExtras] = useState<ExtraRule[]>([]);
  const [submitted, setSubmitted] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const set = <K extends keyof PolicyDraft>(key: K, value: PolicyDraft[K]) =>
    setDraft((d) => ({ ...d, [key]: value }));

  const rulesQuery = useListRegistryRules(undefined, { query: { enabled: !readOnly } });
  const pickableRules = useMemo(
    () => (rulesQuery.data?.data ?? []).filter((r) => r.rule_id !== rule.rule_id && POLICY_RULE_STATUSES.has(r.status)),
    [rulesQuery.data, rule.rule_id],
  );

  const createMutation = useCreateRulePolicies();
  const updateMutation = useUpdateRulePolicy();
  const saving = createMutation.isPending || updateMutation.isPending;

  const allErrors = validateDraft(draft, takenNames, extras);
  const errors = submitted ? allErrors : {};
  const previewBlocked = Object.keys(allErrors).some((k) => k !== "name");

  const setTableKind = (kind: TableConditionDraft["kind"]) => {
    if (kind === draft.tableCondition.kind) return;
    set(
      "tableCondition",
      kind === "none" ? { kind } : kind === "tags" ? { kind, tags: [] } : { kind, rows: [defaultRow("table")] },
    );
  };

  const addRules = (added: RegistryRuleOut[]) =>
    setExtras((prev) => [
      ...prev,
      // Prefilled from each added rule's own slot tags, not copied from this rule.
      ...added
        .filter((r) => !prev.some((e) => e.ruleId === r.rule_id))
        .map((r) => ({ rule: r, ruleId: r.rule_id, columnConditions: defaultColumnConditions(r) })),
    ]);

  const invalidate = () => queryClient.invalidateQueries({ queryKey: getListRulePoliciesQueryKey() });

  const submit = async () => {
    setSubmitted(true);
    if (Object.keys(allErrors).length > 0) {
      document.getElementById("policy-section-name")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    const name = draft.name.trim();
    const n = extras.length;
    try {
      if (isEdit && draft.policyId) {
        await updateMutation.mutateAsync({ policyId: draft.policyId, data: toUpdatePayload(draft) });
        if (n > 0) await createMutation.mutateAsync({ data: toCreatePayload(draft, extras, { includeOwnRule: false }) });
        toast.success(n > 0 ? t("rulePolicies.savedWithCopies", { name, count: n }) : t("rulePolicies.saved", { name }));
      } else {
        await createMutation.mutateAsync({ data: toCreatePayload(draft, extras) });
        toast.success(n > 0 ? t("rulePolicies.createdMany", { count: n + 1 }) : t("rulePolicies.created", { name }));
      }
      await invalidate();
      onClose();
    } catch (err) {
      await invalidate();
      toast.error(extractApiError(err, t("rulePolicies.saveFailed")), { duration: 6000 });
    }
  };

  const n = extras.length;
  const saveLabel = isEdit
    ? n === 0
      ? t("rulePolicies.form.save")
      : t("rulePolicies.form.saveAndCopy", { count: n })
    : n === 0
      ? t("rulePolicies.form.create")
      : t("rulePolicies.form.createMany", { count: n + 1 });

  const groups = [
    {
      rule,
      value: draft.columnConditions,
      onChange: (v: ColumnConditionsDraft) => set("columnConditions", v),
    },
    ...extras.map((e) => ({
      rule: e.rule,
      value: e.columnConditions,
      onChange: (v: ColumnConditionsDraft) =>
        setExtras((prev) => prev.map((x) => (x.ruleId === e.ruleId ? { ...x, columnConditions: v } : x))),
    })),
  ];

  const previewTargets: PreviewTarget[] = groups.map((g) => ({
    ruleId: g.rule.rule_id,
    ruleName: ruleName(g.rule),
    hasSlots: (g.rule.definition.slots ?? []).length > 0,
    draft: { ...draft, ruleId: g.rule.rule_id, columnConditions: g.value },
  }));

  const showErrors = submitted;

  return (
    <div className="pb-4">
      <button
        type="button"
        onClick={onClose}
        className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" /> {t("rulePolicies.form.back")}
      </button>
      <div className="mt-3 mb-2">
        <h2 className="text-xl font-semibold">{isEdit ? initial.name : t("rulePolicies.form.newTitle")}</h2>
        <p className="text-sm text-muted-foreground">
          <Trans
            i18nKey="rulePolicies.form.subtitle"
            values={{ rule: ruleName(rule) }}
            components={[<span key="rule" className="font-medium text-foreground" />]}
          />
        </p>
      </div>
      {readOnly && (
        <div className="mb-2 flex items-center gap-2 rounded-md border bg-muted/40 px-3 py-2 text-sm text-muted-foreground">
          <Lock className="h-4 w-4 shrink-0" />
          {t("rulePolicies.needModify")}
        </div>
      )}
      <Separator />

      <fieldset disabled={readOnly} className="min-w-0">
        <FormSection
          id="policy-section-name"
          title={t("rulePolicies.form.identTitle")}
          description={t("rulePolicies.form.identDescription")}
        >
          <div className="space-y-1.5">
            <label htmlFor="policy-name" className="text-sm font-medium">
              {t("rulePolicies.form.name")}
              <span className="text-destructive">*</span>
            </label>
            <Input
              id="policy-name"
              value={draft.name}
              maxLength={200}
              onChange={(e) => set("name", e.target.value)}
              placeholder={t("rulePolicies.form.namePlaceholder")}
              aria-invalid={!!errors.name}
            />
            <FieldError message={errors.name ? t(errors.name) : undefined} />
          </div>
          <div className="space-y-1.5">
            <label htmlFor="policy-description" className="text-sm font-medium">
              {t("rulePolicies.form.description")}
            </label>
            <Textarea
              id="policy-description"
              value={draft.description}
              maxLength={2000}
              onChange={(e) => set("description", e.target.value)}
              placeholder={t("rulePolicies.form.descriptionPlaceholder")}
              rows={3}
            />
          </div>
        </FormSection>
        <Separator />

        <FormSection title={t("rulePolicies.form.rulesTitle")} description={t("rulePolicies.form.rulesDescription")}>
          <div className="flex flex-wrap items-center gap-2">
            <RuleChip rule={rule} />
            {extras.map((e) => (
              <RuleChip
                key={e.ruleId}
                rule={e.rule}
                onRemove={readOnly ? undefined : () => setExtras((prev) => prev.filter((x) => x.ruleId !== e.ruleId))}
              />
            ))}
          </div>
          {!readOnly && (
            <>
              <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setPickerOpen(true)}>
                <Plus className="h-4 w-4" /> {t("rulePolicies.form.addRules")}
              </Button>
              <PolicyRulesDialog
                open={pickerOpen}
                onOpenChange={setPickerOpen}
                rules={pickableRules}
                alreadyAddedIds={new Set(extras.map((e) => e.ruleId))}
                labelDefinitions={labelDefinitions}
                isLoading={rulesQuery.isLoading}
                isError={rulesQuery.isError}
                onRetry={() => void rulesQuery.refetch()}
                onAdd={addRules}
              />
            </>
          )}
        </FormSection>
        <Separator />

        <FormSection title={t("rulePolicies.form.scopeTitle")} description={t("rulePolicies.form.scopeDescription")}>
          <ScopeFields value={draft.scope} onChange={(scope) => set("scope", scope)} disabled={readOnly} />
          <FieldError message={errors.scope ? t(errors.scope) : undefined} />
          <label className="flex cursor-pointer items-start gap-2.5 rounded-md border bg-card p-3">
            <Checkbox
              checked={draft.registerTables}
              onCheckedChange={(v) => set("registerTables", v === true)}
              className="mt-0.5"
              aria-label={t("rulePolicies.form.registerTables")}
            />
            <span className="space-y-0.5">
              <span className="block text-sm font-medium">{t("rulePolicies.form.registerTables")}</span>
              <span className="block text-xs text-muted-foreground">{t("rulePolicies.form.registerTablesHint")}</span>
            </span>
          </label>
        </FormSection>
        <Separator />

        <FormSection
          title={t("rulePolicies.form.tableConditionsTitle")}
          description={t("rulePolicies.form.tableConditionsDescription")}
        >
          <ChoiceTiles
            ariaLabel={t("rulePolicies.form.tableConditionAria")}
            value={draft.tableCondition.kind}
            onChange={setTableKind}
            disabled={readOnly}
            options={[
              { value: "none", label: t("rulePolicies.form.tileNone"), icon: Ban, info: t("rulePolicies.form.tileNoneInfo") },
              { value: "tags", label: t("rulePolicies.form.tileTableTags"), icon: Tag },
              { value: "builder", label: t("rulePolicies.form.tileCustom"), icon: SlidersHorizontal },
            ]}
          />
          {draft.tableCondition.kind === "tags" && (
            <TagMultiSelect
              value={draft.tableCondition.tags}
              onChange={(tags) => set("tableCondition", { kind: "tags", tags })}
              invalid={!!errors.tableCondition}
              disabled={readOnly}
            />
          )}
          {draft.tableCondition.kind === "builder" && (
            <ConditionRows
              rows={draft.tableCondition.rows}
              onChange={(rows) => set("tableCondition", { kind: "builder", rows })}
              level="table"
              showErrors={showErrors}
              disabled={readOnly}
            />
          )}
          <FieldError message={errors.tableCondition ? t(errors.tableCondition) : undefined} />
        </FormSection>
        <Separator />

        <FormSection
          title={t("rulePolicies.form.columnConditionsTitle")}
          description={t("rulePolicies.form.columnConditionsDescription")}
        >
          {groups.map((g) => (
            <RuleColumnConditions
              key={g.rule.rule_id}
              rule={g.rule}
              value={g.value}
              onChange={g.onChange}
              errors={errors}
              showErrors={showErrors}
              disabled={readOnly}
            />
          ))}
        </FormSection>
        <Separator />

        <FormSection title={t("rulePolicies.form.previewTitle")} description={t("rulePolicies.form.previewDescription")}>
          <PolicyPreview targets={previewTargets} blocked={previewBlocked} />
        </FormSection>
      </fieldset>

      <div className="sticky bottom-0 z-10 -mx-6 flex items-center gap-2 border-t bg-background/95 px-6 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        {readOnly ? (
          <Button variant="outline" onClick={onClose}>
            {t("rulePolicies.form.close")}
          </Button>
        ) : (
          <>
            <Button onClick={() => void submit()} disabled={saving} className="gap-1.5">
              {saving && <Loader2 className="h-4 w-4 animate-spin" />}
              {saveLabel}
            </Button>
            <Button variant="outline" onClick={onClose} disabled={saving}>
              {t("common.cancel")}
            </Button>
            {submitted && Object.keys(errors).length > 0 && (
              <span className="ml-2 text-xs font-medium text-destructive">{t("rulePolicies.form.fixErrors")}</span>
            )}
          </>
        )}
      </div>
    </div>
  );
}
