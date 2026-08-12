import yaml from "js-yaml";
import {
  type CheckFunctionDef,
  type CreateRegistryRuleIn,
  type CreateRegistryRuleInAuthorKind,
  type RuleDefinition,
} from "@/lib/api";
import { batchImportRegistryRulesWithDedup } from "@/lib/api-custom";
import { RESERVED_NAME_KEY } from "@/components/RegistryRuleBadges";
import { parseDqxCheckJson } from "@/lib/registry-rule-conversion";

/**
 * Naming convention for cross-table SQL rules exported from the active-rules
 * page: ``function: __sql_check__/<rule_name>``. On import we recover the
 * canonical ``sql_query`` function and surface the rule name in metadata.
 */
export const SQL_CHECK_PREFIX = "__sql_check__/";

const EMPTY_DEFINITION: RuleDefinition = { body: {}, slots: [], parameters: [] };

export interface ImportRegistryRulesResult {
  saved: number;
  /** Rules matched to an existing active rule by fingerprint — reused, not created. */
  reused: number;
  submitted: number;
  submitFailed: number;
  failed: number;
  errors: string[];
}

export interface ParseImportYamlResult {
  checks: Record<string, unknown>[] | null;
  error: string | null;
  hint: string | null;
}

/** Move legacy top-level ``weight`` into ``user_metadata.weight``. */
function normalizeWeight(item: Record<string, unknown>): void {
  if (typeof item.weight !== "number") return;
  const md: Record<string, string> = {};
  const existing = item.user_metadata;
  if (existing && typeof existing === "object") {
    for (const [k, v] of Object.entries(existing as Record<string, unknown>)) {
      if (typeof v === "string") md[k] = v;
    }
  }
  if (!("weight" in md)) md.weight = String(item.weight);
  item.user_metadata = md;
  delete item.weight;
}

/**
 * Normalize a YAML or contract check dict into the DQX check JSON shape
 * {@link parseDqxCheckJson} expects.
 */
export function normalizeImportedCheck(raw: Record<string, unknown>): Record<string, unknown> {
  const item = { ...raw };
  normalizeWeight(item);

  const checkBlock = (item.check as Record<string, unknown>) ?? item;
  const inner = { ...checkBlock };
  let fn = String(inner.function ?? "");

  const userMetadata: Record<string, string> = {};
  const existing = item.user_metadata;
  if (existing && typeof existing === "object") {
    for (const [k, v] of Object.entries(existing as Record<string, unknown>)) {
      if (typeof v === "string") userMetadata[k] = v;
    }
  }
  if (typeof item.name === "string" && item.name) {
    userMetadata[RESERVED_NAME_KEY] = item.name;
  }

  if (fn.startsWith(SQL_CHECK_PREFIX)) {
    const ruleName = fn.slice(SQL_CHECK_PREFIX.length);
    if (ruleName && !userMetadata[RESERVED_NAME_KEY]) {
      userMetadata[RESERVED_NAME_KEY] = ruleName;
    }
    fn = "sql_query";
    inner.function = fn;
  }

  const criticality = String(item.criticality ?? inner.criticality ?? "warn");
  const messageExpr = typeof item.message_expr === "string" ? item.message_expr : undefined;

  const result: Record<string, unknown> = {
    criticality,
    check: {
      function: fn,
      arguments:
        typeof inner.arguments === "object" && inner.arguments !== null && !Array.isArray(inner.arguments)
          ? inner.arguments
          : {},
    },
    user_metadata: userMetadata,
  };
  if (messageExpr) result.message_expr = messageExpr;
  if (typeof item.filter === "string" && item.filter.trim()) {
    result.filter = item.filter.trim();
  }
  // Prefer `owner`; still accept legacy YAML `steward` when reading.
  const ownerFromTop =
    typeof item.owner === "string" && item.owner.trim()
      ? item.owner.trim()
      : typeof item.steward === "string" && item.steward.trim()
        ? item.steward.trim()
        : undefined;
  const ownerFromMeta =
    typeof userMetadata.owner === "string" && userMetadata.owner.trim()
      ? userMetadata.owner.trim()
      : typeof userMetadata.steward === "string" && userMetadata.steward.trim()
        ? userMetadata.steward.trim()
        : undefined;
  const owner = ownerFromTop ?? ownerFromMeta;
  if (owner) {
    result.owner = owner;
    // Owner is a first-class Rules Registry field, not a free-text tag.
    delete userMetadata.owner;
    delete userMetadata.steward;
  }
  return result;
}

function isRuleObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function coerceToRuleList(parsed: unknown): Record<string, unknown>[] | null {
  if (Array.isArray(parsed)) {
    return parsed as Record<string, unknown>[];
  }
  if (!isRuleObject(parsed)) return null;

  const wrapper = parsed;
  if (Array.isArray(wrapper.checks)) {
    return wrapper.checks as Record<string, unknown>[];
  }
  if ("check" in wrapper || "name" in wrapper || "criticality" in wrapper) {
    return [wrapper];
  }
  return null;
}

/**
 * Parse pasted/uploaded YAML into normalized check dicts for import.
 * Surfaces syntax errors and common shape mistakes instead of failing silently.
 */
export function parseImportYamlText(
  text: string,
  messages: {
    yamlMustBeList: string;
    commentsOnly: string;
    emptyList: string;
    invalidEntry: string;
  },
): ParseImportYamlResult {
  const trimmed = text.trim();
  if (!trimmed || trimmed === "-") {
    return { checks: null, error: null, hint: null };
  }

  let parsed: unknown;
  try {
    parsed = yaml.load(text);
  } catch (err) {
    return {
      checks: null,
      error: err instanceof Error ? err.message : String(err),
      hint: null,
    };
  }

  if (parsed == null) {
    return { checks: null, error: null, hint: messages.commentsOnly };
  }

  const rules = coerceToRuleList(parsed);
  if (rules == null) {
    return { checks: null, error: messages.yamlMustBeList, hint: null };
  }

  if (rules.length === 0) {
    return { checks: null, error: null, hint: messages.emptyList };
  }

  const invalidIndex = rules.findIndex((item) => !isRuleObject(item));
  if (invalidIndex >= 0) {
    return {
      checks: null,
      error: null,
      hint: messages.invalidEntry.replace("{{index}}", String(invalidIndex + 1)),
    };
  }

  const normalized = rules.map((raw) => normalizeImportedCheck(raw));
  return { checks: normalized, error: null, hint: null };
}

/**
 * Convert already-{@link normalizeImportedCheck}-normalized check dicts into
 * {@link CreateRegistryRuleIn} drafts.
 *
 * Callers MUST normalize once, at the import source, before handing checks
 * here (YAML via {@link parseImportYamlText}; data contracts at the point the
 * generated rules are collected). Normalizing again here would be a second
 * pass over the same dicts — harmless while {@link normalizeImportedCheck} is
 * idempotent, but a future non-idempotent change would then silently corrupt
 * one source and not the other. Keep normalization in exactly one place per
 * source.
 */
export function parseChecksForImport(
  checks: Record<string, unknown>[],
  checkFunctions: CheckFunctionDef[],
  t: (key: string, opts?: Record<string, unknown>) => string,
  authorKind: CreateRegistryRuleInAuthorKind,
): { rules: CreateRegistryRuleIn[]; errors: string[] } {
  const rules: CreateRegistryRuleIn[] = [];
  const errors: string[] = [];

  for (const normalized of checks) {
    try {
      const parsed = parseDqxCheckJson(
        JSON.stringify(normalized),
        EMPTY_DEFINITION,
        {},
        checkFunctions,
        t,
      );
      // Checks tagged ``rule_type: text_llm`` were produced by the retired
      // LLM contract-import leg. Nothing emits the tag now, but rules saved
      // or exported earlier still carry it and must keep AI provenance.
      const isLlmDerived = parsed.userMetadata?.["rule_type"] === "text_llm";
      // Prefer `owner`; still accept legacy YAML `steward` when reading.
      const owner =
        typeof normalized.owner === "string" && normalized.owner.trim()
          ? normalized.owner.trim()
          : typeof normalized.steward === "string" && normalized.steward.trim()
            ? normalized.steward.trim()
            : typeof parsed.userMetadata?.["owner"] === "string" && parsed.userMetadata["owner"].trim()
              ? parsed.userMetadata["owner"].trim()
              : typeof parsed.userMetadata?.["steward"] === "string" &&
                  parsed.userMetadata["steward"].trim()
                ? parsed.userMetadata["steward"].trim()
                : undefined;
      if (owner && parsed.userMetadata) {
        delete parsed.userMetadata["owner"];
        delete parsed.userMetadata["steward"];
      }
      rules.push({
        mode: parsed.mode,
        definition: parsed.definition,
        polarity: parsed.polarity,
        user_metadata: parsed.userMetadata,
        author_kind: isLlmDerived ? "ai_assisted" : authorKind,
        ...(owner ? { owner } : {}),
      });
    } catch (err) {
      errors.push(err instanceof Error ? err.message : String(err));
    }
  }

  return { rules, errors };
}

/**
 * Convert imported check dicts into Rules Registry drafts (and optionally
 * submit each for review) via a single batch API call.
 *
 * `checks` are expected to be already normalized with
 * {@link normalizeImportedCheck} at their source (see
 * {@link parseChecksForImport}); this function does not normalize again.
 */
export async function importChecksAsRegistryDrafts({
  checks,
  checkFunctions,
  t,
  owner = null,
  authorKind = "human",
  alsoSubmit = false,
  autoApprove = false,
  source,
}: {
  checks: Record<string, unknown>[];
  checkFunctions: CheckFunctionDef[];
  t: (key: string, opts?: Record<string, unknown>) => string;
  owner?: string | null;
  authorKind?: CreateRegistryRuleInAuthorKind;
  alsoSubmit?: boolean;
  /** Publish imported rules outright (submit + approve) — requires an approver
   *  role server-side. Used by the admin-only Marketplace. */
  autoApprove?: boolean;
  /** Provenance recorded on each rule (RuleSourceBadge). Defaults server-side
   *  to "import"; the Marketplace passes "marketplace". */
  source?: string;
}): Promise<ImportRegistryRulesResult> {
  const { rules, errors: parseErrors } = parseChecksForImport(checks, checkFunctions, t, authorKind);

  if (rules.length === 0) {
    return {
      saved: 0,
      reused: 0,
      submitted: 0,
      submitFailed: 0,
      failed: parseErrors.length,
      errors: parseErrors,
    };
  }

  const rulesWithOwner = rules.map((rule) => {
    if (rule.owner) return rule;
    return owner ? { ...rule, owner } : rule;
  });

  // skip_duplicates makes re-imports idempotent: a structurally-identical
  // active rule (draft/pending/approved) is reused instead of minting a copy.
  const resp = await batchImportRegistryRulesWithDedup({
    rules: rulesWithOwner,
    also_submit: alsoSubmit,
    auto_approve: autoApprove,
    skip_duplicates: true,
    ...(source ? { source } : {}),
  });

  const data = resp.data;
  // The orval-generated BatchImportRegistryRulesOut types every field as
  // optional (the backend model gives them defaults), so guard against
  // undefined rather than assuming a shape.
  const failedList = data.failed ?? [];
  const serverErrors = failedList.map((f) => f.error);

  return {
    saved: data.saved ?? 0,
    reused: (data.reused ?? []).length,
    submitted: data.submitted ?? 0,
    submitFailed: data.submit_failed ?? 0,
    failed: parseErrors.length + failedList.length,
    errors: [...parseErrors, ...serverErrors],
  };
}
