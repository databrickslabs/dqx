import type { RuleParameter, RuleParameterValue } from "@/lib/api";

const PLACEHOLDER_RE = /^\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$/;
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const SQL_PARAM_NAME_RE =
  /(max_length|min_length|min_limit|max_limit|min_value|max_value|regex|pattern|decimal_scale|\bscale\b|\blimit\b|\bthreshold\b|allowed|launch_date|_rate)/i;

/** Color for `{{column}}` tokens in the rule editor and apply mapping. */
export const COLUMN_PLACEHOLDER_CLASS = "font-mono text-blue-600 dark:text-blue-400";
/** Color for `{{parameter}}` tokens — distinct from column placeholders. */
export const PARAM_PLACEHOLDER_CLASS = "font-mono text-amber-700 dark:text-amber-400";
const POSITIVE_INT_NAMES = new Set(["limit", "max_length", "min_length", "column_max_length"]);
const NONNEG_INT_NAMES = new Set(["scale", "decimal_scale"]);
const RANGE_PAIRS: Array<[string, string]> = [
  ["min_limit", "max_limit"],
  ["min_value", "max_value"],
  ["min", "max"],
];

export function isPlaceholderString(value: unknown): boolean {
  return typeof value === "string" && PLACEHOLDER_RE.test(value.trim());
}

export function isUnboundParameterValue(value: unknown): boolean {
  return value === null || value === undefined || isPlaceholderString(value);
}

export function isSqlParamPlaceholderName(name: string): boolean {
  return SQL_PARAM_NAME_RE.test(name) && !/^columns?_?\d*$/i.test(name);
}

export function mappingGroupKey(group: Record<string, string>): string {
  return JSON.stringify(
    Object.fromEntries(Object.entries(group).sort(([a], [b]) => a.localeCompare(b))),
  );
}

export function displayParameterValue(value: RuleParameterValue | undefined): string {
  if (isUnboundParameterValue(value)) return "";
  if (Array.isArray(value)) return value.join(", ");
  if (value === true) return "true";
  if (value === false) return "false";
  return String(value);
}

function looksLikeLengthParam(name: string): boolean {
  const lowered = name.toLowerCase();
  return POSITIVE_INT_NAMES.has(lowered) || lowered.includes("max_length") || lowered.includes("min_length");
}

function looksLikeScaleParam(name: string): boolean {
  const lowered = name.toLowerCase();
  return NONNEG_INT_NAMES.has(lowered) || lowered.endsWith("_scale");
}

function looksLikeRateParam(name: string): boolean {
  return name.toLowerCase().includes("rate");
}

function asNumber(name: string, value: unknown): number {
  if (typeof value === "boolean" || (typeof value !== "number" && typeof value !== "string")) {
    throw new Error(`${name} must be a number.`);
  }
  if (typeof value === "string" && !value.trim()) {
    throw new Error(`${name} is required.`);
  }
  const number = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(number)) {
    throw new Error(`${name} must be a number.`);
  }
  return number;
}

function asInt(name: string, value: unknown, minimum: number | null): number {
  const number = asNumber(name, value);
  if (!Number.isInteger(number)) {
    throw new Error(`${name} must be a whole number.`);
  }
  if (minimum !== null && number < minimum) {
    throw new Error(
      minimum <= 0
        ? `${name} must be zero or a positive whole number.`
        : `${name} must be a positive whole number.`,
    );
  }
  return number;
}

export function coerceParameterValue(
  param: Pick<RuleParameter, "name" | "type">,
  raw: unknown,
): RuleParameterValue {
  if (isUnboundParameterValue(raw)) {
    throw new Error(`${param.name} is required.`);
  }
  if (typeof raw === "string" && !raw.trim() && param.type !== "boolean") {
    throw new Error(`${param.name} is required.`);
  }

  if (param.type === "boolean") {
    if (typeof raw === "boolean") return raw;
    if (typeof raw === "string" && ["true", "false"].includes(raw.trim().toLowerCase())) {
      return raw.trim().toLowerCase() === "true";
    }
    throw new Error(`${param.name} must be true or false.`);
  }

  if (param.type === "list" || param.type === "ref_column") {
    const items = Array.isArray(raw)
      ? raw.map((item) => String(item).trim()).filter(Boolean)
      : typeof raw === "string"
        ? raw.split(",").map((part) => part.trim()).filter(Boolean)
        : null;
    if (!items || items.length === 0) {
      throw new Error(`${param.name} is required.`);
    }
    if (items.some((item) => isPlaceholderString(item))) {
      throw new Error(`${param.name} cannot be a placeholder.`);
    }
    return items;
  }

  if (param.type === "number" || param.type === "integer" || param.type === "decimal") {
    if (looksLikeLengthParam(param.name)) return asInt(param.name, raw, 1);
    if (looksLikeScaleParam(param.name)) return asInt(param.name, raw, 0);
    if (param.type === "integer") return asInt(param.name, raw, null);
    const number = asNumber(param.name, raw);
    if (looksLikeRateParam(param.name) && (number < 0 || number > 100)) {
      throw new Error(`${param.name} must be between 0 and 100.`);
    }
    return number;
  }

  if (param.type === "date") {
    const text = String(raw).trim().slice(0, 10);
    const parsed = new Date(`${text}T00:00:00Z`);
    if (!ISO_DATE_RE.test(text) || Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== text) {
      throw new Error(`${param.name} must be a date (YYYY-MM-DD).`);
    }
    return text;
  }

  const text = String(raw).trim();
  if (!text) throw new Error(`${param.name} is required.`);
  if (isPlaceholderString(text)) {
    throw new Error(`${param.name} cannot be a placeholder.`);
  }
  return text;
}

/** SQL text references each parameter as a `{{token}}`, so none may stay unbound. */
export function bindsEveryParameter(mode: string | null | undefined): boolean {
  return mode === "sql" || mode === "lowcode";
}

export function mustBindParameter(param: RuleParameter, bindAll = false): boolean {
  if (bindAll || isPlaceholderString(param.value)) return true;
  if (param.type === "integer" || param.type === "decimal" || param.type === "date") return true;
  if (isSqlParamPlaceholderName(param.name)) return true;
  const lowered = param.name.toLowerCase();
  if (
    [
      "limit",
      "regex",
      "pattern",
      "allowed",
      "min_limit",
      "max_limit",
      "min_value",
      "max_value",
      "scale",
      "decimal_scale",
      "column_max_length",
      "max_length",
      "min_length",
      "ref_table",
      "ref_columns",
    ].includes(lowered)
  ) {
    return true;
  }
  return looksLikeLengthParam(param.name) || looksLikeScaleParam(param.name) || looksLikeRateParam(param.name);
}

export function validateGroupParameterValues(
  parameters: RuleParameter[],
  rawValues: Record<string, unknown> | undefined,
  bindAll = false,
): { values: Record<string, RuleParameterValue>; error: string | null } {
  if (parameters.length === 0) return { values: {}, error: null };
  const coerced: Record<string, RuleParameterValue> = {};
  try {
    for (const param of parameters) {
      const raw = rawValues?.[param.name] ?? (isUnboundParameterValue(param.value) ? undefined : param.value);
      if (isUnboundParameterValue(raw) && !mustBindParameter(param, bindAll)) continue;
      coerced[param.name] = coerceParameterValue(param, raw);
    }
    for (const [minName, maxName] of RANGE_PAIRS) {
      if (!(minName in coerced) || !(maxName in coerced)) continue;
      const minimum = Number(coerced[minName]);
      const maximum = Number(coerced[maxName]);
      if (Number.isFinite(minimum) && Number.isFinite(maximum) && minimum > maximum) {
        throw new Error(
          `${minName} (${minimum}) must not be greater than ${maxName} (${maximum}).`,
        );
      }
    }
  } catch (err) {
    return { values: coerced, error: err instanceof Error ? err.message : String(err) };
  }
  return { values: coerced, error: null };
}

/** Split AI-declared `{{token}}`s into column slots vs apply-time parameters. */
export function partitionPlaceholderSlots<T extends { name: string }>(
  items: T[],
): {
  columns: T[];
  parameters: RuleParameter[];
} {
  const columns: T[] = [];
  const parameters: RuleParameter[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    if (seen.has(item.name)) continue;
    seen.add(item.name);
    if (isSqlParamPlaceholderName(item.name)) {
      parameters.push({ name: item.name, type: inferSqlParamType(item.name), value: null });
    } else {
      columns.push(item);
    }
  }
  return { columns, parameters };
}

export function inferSqlParamType(name: string): RuleParameter["type"] {
  const lowered = name.toLowerCase();
  if (looksLikeLengthParam(name) || looksLikeScaleParam(name)) return "integer";
  if (
    looksLikeRateParam(name) ||
    ["min_limit", "max_limit", "min_value", "max_value"].some((part) => lowered.includes(part))
  ) {
    return "decimal";
  }
  if (lowered.includes("date")) return "date";
  if (lowered.includes("allowed")) return "list";
  if (lowered.includes("regex") || lowered.includes("pattern")) return "regex";
  return "string";
}

/** Parameter types an author can declare on a SQL rule, in picker order. */
export const SQL_PARAMETER_TYPES = ["integer", "decimal", "date", "string", "list", "boolean", "regex"] as const;

/** The Test-rule cell family that renders the right input for a parameter type. */
export function parameterInputFamily(type: RuleParameter["type"]): "numeric" | "temporal" | "boolean" | "text" {
  if (type === "number" || type === "integer" || type === "decimal") return "numeric";
  if (type === "date") return "temporal";
  if (type === "boolean") return "boolean";
  return "text";
}
