/**
 * Shared conversion helpers between a Rules Registry rule's structured
 * `definition` (mode/body/slots/parameters) and two other representations:
 *
 * - The visual form fields in {@link RegistryRuleFormDialog} (function +
 *   slot pickers + parameter inputs).
 * - The native DQX check-dict JSON shown/edited in
 *   {@link RegistryRuleJsonDialog} — the exact `{ check: { function,
 *   arguments } }` shape `apply_checks_by_metadata` consumes, with reusable
 *   slots left as `{{slot_name}}` placeholders (a registry rule is
 *   table-agnostic, so it is never rendered against real columns here).
 *   Also includes `user_metadata` — the rule's reserved tags
 *   (name/description/dimension/severity) plus any free-text tags — since
 *   that dict IS what `render_check` stamps into the materialized
 *   `dq_quality_rules.check` row (minus the per-application provenance keys
 *   — `registry_rule_id`, `registry_version`, `applied_rule_id`,
 *   `polarity` — that only exist once a rule is applied to a table and
 *   therefore have no meaning on a still-unattached registry rule).
 *
 * Centralized here (rather than duplicated in each dialog) so the
 * definition <-> function-signature mapping has exactly one implementation —
 * see AGENTS.md's DRY principle. Mirrors the backend's
 * `backend/services/materializer.py::render_check` (Python) for the
 * definition -> check-dict direction; there is no backend equivalent for the
 * reverse (check-dict -> definition) direction because that's authoring-time
 * UI logic, not runtime rendering.
 */
import type {
  CheckFunctionDef as ApiCheckFunctionDef,
  RegistryRuleOut,
  RuleDefinition,
  RuleParameter,
  RuleParameterType,
  RuleSlot,
} from "@/lib/api";
import { RESERVED_NAME_KEY, RESERVED_SEVERITY_KEY, getTag } from "@/components/RegistryRuleBadges";

export const COLUMN_KINDS = new Set(["column", "columns"]);
export const PARAM_KIND_TO_TYPE: Record<string, RuleParameterType> = {
  boolean: "boolean",
  number: "number",
  list: "list",
  string: "string",
  ref_table: "ref_table",
  ref_columns: "ref_column",
};

/** Mirrors `registry_models.SEVERITY_TO_CRITICALITY` / `resolve_criticality`. */
const SEVERITY_TO_CRITICALITY: Record<string, "warn" | "error"> = {
  Low: "warn",
  Medium: "warn",
  High: "error",
  Critical: "error",
};

export function resolveCriticality(severity: string | undefined): "warn" | "error" {
  if (!severity) return "warn";
  return SEVERITY_TO_CRITICALITY[severity] ?? "warn";
}

export function deriveSlotsAndParameters(fn: ApiCheckFunctionDef | undefined): {
  slots: RuleSlot[];
  parameters: RuleParameter[];
} {
  if (!fn) return { slots: [], parameters: [] };
  const slots: RuleSlot[] = [];
  const parameters: RuleParameter[] = [];
  let position = 0;
  let columnIndex = 1;
  for (const p of fn.params ?? []) {
    if (COLUMN_KINDS.has(p.kind)) {
      // Every column-kind parameter is seeded with ONE `column_N`-named
      // slot bound one-to-one to a real column (`cardinality: "one"`) —
      // matching the SQL/Low-Code slot-naming scheme (`nextSlotName`) so a
      // fresh native rule reads `{{column_1}}` instead of the function's
      // raw parameter name. A "many"/list-kind parameter (e.g.
      // `foreign_key`'s `columns`) still starts with a single slot, but the
      // author can add MORE `column_N` slots sharing this same `arg_key`
      // via `SlotsPanel`'s expandable group (see `listArgKeys` in
      // `nativeArguments` below and `expandableArgKey` in
      // `RegistryRuleFormDialog`) — each slot maps one real column, and
      // together they populate the function's list argument.
      slots.push({
        name: `column_${columnIndex++}`,
        family: "any",
        position: position++,
        cardinality: "one",
        arg_key: p.name,
      });
    } else {
      parameters.push({
        name: p.name,
        type: PARAM_KIND_TO_TYPE[p.kind] ?? "string",
        value: null,
      });
    }
  }
  return { slots, parameters };
}

/** The `arg_key` of *fn*'s column parameter that accepts a LIST of columns
 * (`kind: "columns"`, e.g. `foreign_key`'s `columns`), or `undefined` if
 * *fn* has no such parameter. Drives both {@link nativeArguments}' list-vs-
 * scalar argument shape and `SlotsPanel`'s expandable multi-slot group. */
export function listColumnArgKey(fn: ApiCheckFunctionDef | undefined): string | undefined {
  return fn?.params?.find((p) => p.kind === "columns")?.name;
}

/**
 * Every declared slot becomes a `{{slot_name}}` placeholder — a registry rule's native
 * `arguments` template is never bound to a real column. The dict KEY is the slot's
 * `arg_key` (the DQX function's real parameter name — falls back to `slot.name` for
 * slots that predate `arg_key`); the VALUE is always the author's `{{name}}` placeholder,
 * so an author-renamed slot still fills the correct function argument.
 *
 * A `arg_key` shared by MORE THAN ONE slot (a multi-column group, e.g.
 * `foreign_key`'s `columns`) always renders as a LIST of placeholders in
 * slot `position` order — `_substitute_arguments` on the backend already
 * substitutes each list element independently. A SINGLE slot for an
 * `arg_key` that *fn*'s signature marks as list-typed (`kind: "columns"`,
 * see {@link listColumnArgKey}) still renders as a one-element list, since
 * the function itself expects a list argument regardless of how many
 * columns the author has declared so far. Every other single slot renders
 * as a bare placeholder string, INCLUDING a legacy `cardinality: "many"`
 * slot predating this multi-slot design — that shape is preserved as-is
 * so already-published rules keep materializing via the older
 * comma-separated-value substitution path in `_substitute_value`.
 */
export function nativeArguments(slots: RuleSlot[], fn?: ApiCheckFunctionDef): Record<string, unknown> {
  const listArgKey = listColumnArgKey(fn);
  const groups = new Map<string, RuleSlot[]>();
  for (const s of slots) {
    const key = s.arg_key ?? s.name;
    const members = groups.get(key);
    if (members) members.push(s);
    else groups.set(key, [s]);
  }
  const args: Record<string, unknown> = {};
  for (const [key, members] of groups) {
    if (members.length > 1) {
      const ordered = members.slice().sort((a, b) => (a.position ?? 0) - (b.position ?? 0));
      args[key] = ordered.map((s) => `{{${s.name}}}`);
      continue;
    }
    const slot = members[0];
    args[key] = slot.cardinality === "one" && key === listArgKey ? [`{{${slot.name}}}`] : `{{${slot.name}}}`;
  }
  return args;
}

export function parseParamValue(type: RuleParameterType, raw: string): RuleParameter["value"] {
  const trimmed = raw.trim();
  if (trimmed === "") return null;
  switch (type) {
    case "boolean":
      return trimmed === "true";
    case "number": {
      const n = Number(trimmed);
      return Number.isNaN(n) ? null : n;
    }
    case "list":
    case "ref_column":
      return trimmed
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
    default:
      return trimmed;
  }
}

export function paramValueToRaw(value: RuleParameter["value"]): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(", ");
  return String(value);
}

const SQL_FUNCTION_NAMES = new Set(["sql_query", "sql_expression"]);

/**
 * Derive the native DQX check-dict for *rule* — the exact shape
 * `apply_checks_by_metadata` consumes, with slots left as `{{slot}}`
 * placeholders (mirrors `materializer.render_check` before column
 * substitution, since a registry rule is table-agnostic). Includes
 * `user_metadata` — *rule*'s own tags dict, unchanged — so the JSON shown
 * to the user faithfully mirrors what flows into the materialized
 * `dq_quality_rules.check` row (see the module docstring for exactly which
 * per-application keys are intentionally excluded).
 */
export function buildDqxCheckJson(rule: RegistryRuleOut): Record<string, unknown> {
  const definition = rule.definition ?? ({} as RuleDefinition);
  const body = (definition.body ?? {}) as Record<string, unknown>;
  const parameters = definition.parameters ?? [];

  let checkInner: Record<string, unknown>;
  if (rule.mode === "dqx_native") {
    const args: Record<string, unknown> = { ...(body.arguments as Record<string, unknown> | undefined) };
    for (const p of parameters) {
      if (p.value !== null && p.value !== undefined) args[p.name] = p.value;
    }
    checkInner = { function: String(body.function ?? ""), arguments: args };
  } else {
    const negate = rule.polarity === "fail";
    const args: Record<string, unknown> = { negate };
    let functionName: string;
    if (typeof body.sql_query === "string") {
      functionName = "sql_query";
      args.query = body.sql_query;
      // A low-code advanced (group-by) rule carries the group-by columns as
      // `merge_columns` so violating groups flag their source rows row-level;
      // surface them so the JSON mirrors exactly what materializes.
      if (Array.isArray(body.merge_columns) && body.merge_columns.length > 0) {
        args.merge_columns = body.merge_columns;
      }
    } else {
      functionName = "sql_expression";
      args.expression = typeof body.predicate === "string" ? body.predicate : "";
    }
    for (const p of parameters) {
      if (p.value !== null && p.value !== undefined) args[p.name] = p.value;
    }
    checkInner = { function: functionName, arguments: args };
  }

  const severity = getTag(rule, RESERVED_SEVERITY_KEY);
  const check: Record<string, unknown> = {
    criticality: resolveCriticality(severity || undefined),
    check: checkInner,
    user_metadata: rule.user_metadata ?? {},
  };
  const name = getTag(rule, RESERVED_NAME_KEY);
  if (name) check.name = name;
  if (definition.error_message) check.message_expr = definition.error_message;
  return check;
}

export interface ParsedCheckDefinition {
  mode: "dqx_native" | "sql" | "lowcode";
  definition: RuleDefinition;
  polarity: "pass" | "fail" | null;
  /**
   * The rule's `user_metadata` tags dict as edited in the JSON — round-trips
   * back into the rule's `user_metadata` (the same store the About-tab tag
   * fields write to). Only string-valued entries survive (mirrors the
   * backend's `_build_user_metadata` merge, which likewise drops non-string
   * values); falls back to *currentUserMetadata* unchanged when the `check`
   * JSON has no `user_metadata` object at all (e.g. hand-crafted JSON that
   * never had the key), so a missing key never silently wipes existing tags.
   */
  userMetadata: Record<string, string>;
}

/**
 * Parse a hand-edited DQX check-dict back into a rule `definition`.
 *
 * Validates the function name against *checkFunctions* (the same
 * `CHECK_FUNC_REGISTRY`-backed list the visual form's function picker uses)
 * so an edited JSON can never reference a function the DQX engine doesn't
 * know about. Native slot arguments are always rewritten to canonical
 * `{{slot}}` placeholders regardless of what the user typed there — a
 * registry rule is table-agnostic, so a slot argument can never carry a
 * real value. SQL safety is enforced server-side (`RegistryService.update_draft`)
 * on save, since `is_sql_query_safe` has no frontend equivalent.
 *
 * `user_metadata` round-trips: if the top-level `user_metadata` object is
 * present, it fully REPLACES *currentUserMetadata* (add/edit/remove a tag —
 * including the reserved name/description/dimension/severity keys — and it
 * persists); if the key is absent or not a plain object, *currentUserMetadata*
 * is kept as-is so a save can never silently drop existing tags.
 *
 * Throws a plain `Error` with a user-facing message on any validation
 * failure; callers should catch and surface `error.message` inline.
 */
export function parseDqxCheckJson(
  rawText: string,
  currentDefinition: RuleDefinition,
  currentUserMetadata: Record<string, unknown> | null | undefined,
  checkFunctions: ApiCheckFunctionDef[],
  t: (key: string, opts?: Record<string, unknown>) => string,
  currentMode?: "dqx_native" | "lowcode" | "sql",
): ParsedCheckDefinition {
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawText);
  } catch {
    throw new Error(t("rulesRegistry.jsonParseError"));
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(t("rulesRegistry.jsonShapeError"));
  }
  const dict = parsed as Record<string, unknown>;
  const checkBlock = dict.check;
  if (typeof checkBlock !== "object" || checkBlock === null || Array.isArray(checkBlock)) {
    throw new Error(t("rulesRegistry.jsonShapeError"));
  }
  const checkInner = checkBlock as Record<string, unknown>;
  const functionName = checkInner.function;
  if (typeof functionName !== "string" || !functionName) {
    throw new Error(t("rulesRegistry.jsonShapeError"));
  }
  const fn = checkFunctions.find((f) => f.name === functionName);
  if (!fn) {
    throw new Error(t("rulesRegistry.jsonUnknownFunctionError", { function: functionName }));
  }
  const rawArguments = checkInner.arguments;
  const args: Record<string, unknown> =
    typeof rawArguments === "object" && rawArguments !== null && !Array.isArray(rawArguments)
      ? (rawArguments as Record<string, unknown>)
      : {};

  const errorMessage = typeof dict.message_expr === "string" ? dict.message_expr : undefined;
  const userMetadata = parseUserMetadata(dict.user_metadata, currentUserMetadata);

  if (SQL_FUNCTION_NAMES.has(functionName)) {
    const polarity: "pass" | "fail" = args.negate === true ? "fail" : "pass";
    const body: Record<string, unknown> =
      functionName === "sql_query"
        ? { sql_query: typeof args.query === "string" ? args.query : "" }
        : { predicate: typeof args.expression === "string" ? args.expression : "" };
    if (functionName === "sql_query" && Array.isArray(args.merge_columns)) {
      body.merge_columns = args.merge_columns.filter((c) => typeof c === "string");
    }
    // Preserve LOW-CODE identity when editing a low-code rule's JSON: keep
    // mode "lowcode" and carry the re-editable AST + group-by forward from
    // the stored body (the JSON is only the compiled sql_check view — it
    // never carries the AST), so a JSON round-trip can't silently flip a
    // low-code rule to SQL and drop its structured rows.
    if (currentMode === "lowcode") {
      const currentBody = (currentDefinition.body ?? {}) as Record<string, unknown>;
      if (currentBody.lowcode_ast !== undefined) body.lowcode_ast = currentBody.lowcode_ast;
      if (typeof currentBody.group_by === "string") body.group_by = currentBody.group_by;
      return {
        mode: "lowcode",
        polarity,
        definition: {
          body,
          slots: currentDefinition.slots ?? [],
          parameters: currentDefinition.parameters ?? [],
          error_message: errorMessage,
        },
        userMetadata,
      };
    }
    return {
      mode: "sql",
      polarity,
      definition: {
        body,
        slots: currentDefinition.slots ?? [],
        parameters: currentDefinition.parameters ?? [],
        error_message: errorMessage,
      },
      userMetadata,
    };
  }

  const { slots, parameters: derivedParams } = deriveSlotsAndParameters(fn);
  const parameters: RuleParameter[] = derivedParams.map((p) => {
    const raw = args[p.name];
    const rawStr = raw === undefined || raw === null ? "" : Array.isArray(raw) ? raw.join(", ") : String(raw);
    return { ...p, value: parseParamValue(p.type, rawStr) };
  });

  return {
    mode: "dqx_native",
    polarity: null,
    definition: {
      body: { function: functionName, arguments: nativeArguments(slots, fn) },
      slots,
      parameters,
      error_message: errorMessage,
    },
    userMetadata,
  };
}

/**
 * Resolve the `user_metadata` dict for {@link parseDqxCheckJson}'s result.
 *
 * Only string-valued entries are kept — mirrors the backend's
 * `_build_user_metadata` merge, which likewise drops any non-string key or
 * value rather than rejecting the whole save. When *raw* isn't a plain
 * object (missing, `null`, an array, or any other JSON type), the entire
 * existing tags dict is preserved unchanged so a save can never silently
 * wipe tags the user didn't intend to touch.
 */
function parseUserMetadata(
  raw: unknown,
  current: Record<string, unknown> | null | undefined,
): Record<string, string> {
  const fallback: Record<string, string> = {};
  for (const [k, v] of Object.entries(current ?? {})) {
    if (typeof v === "string") fallback[k] = v;
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return fallback;
  }
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof v === "string") out[k] = v;
  }
  return out;
}
