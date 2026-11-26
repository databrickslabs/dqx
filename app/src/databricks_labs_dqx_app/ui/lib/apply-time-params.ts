import type { RuleParameter, RuleParameterValue } from "@/lib/api";

const PLACEHOLDER_RE = /^\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$/;
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
// Whole-name match (see backend `_SQL_PARAM_NAME_RE`): `{{pattern_id}}`, `{{is_allowed}}` and
// `{{exchange_rate}}` stay column slots.
const SQL_PARAM_NAME_RE =
  /^(?:[a-z0-9]+_)*(?:max_length|min_length|min_limit|max_limit|min_value|max_value|decimal_scale|scale|limit|threshold|regex|pattern)$|^allowed(?:_values?)?$/i;
const RANGE_BOUND_RE = /(?:^|_)(?:min|max)_(?:limit|value)$/;

/** Color for `{{column}}` tokens in the rule editor and apply mapping. */
export const COLUMN_PLACEHOLDER_CLASS = "font-mono text-blue-600 dark:text-blue-400";
/** Color for `{{parameter}}` tokens — distinct from column placeholders. */
export const PARAM_PLACEHOLDER_CLASS = "font-mono text-amber-700 dark:text-amber-400";
// Length parameters match whole name tokens, mirroring the backend. `limit` is deliberately not a
// length: DQX accepts zero, negative and decimal limits and validates them itself.
const MAX_LENGTH_RE = /(?:^|_)max_length(?:_|$)/;
const MIN_LENGTH_RE = /(?:^|_)min_length(?:_|$)/;
const PERCENT_RE = /(?:^|_)(?:pct|percent|percentage)(?:_|$)/;
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
  return SQL_PARAM_NAME_RE.test(name);
}

export function mappingGroupKey(group: Record<string, string>): string {
  return JSON.stringify(Object.fromEntries(Object.entries(group).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))));
}

export function displayParameterValue(value: RuleParameterValue | undefined): string {
  if (isUnboundParameterValue(value)) return "";
  if (Array.isArray(value)) return value.join(", ");
  if (value === true) return "true";
  if (value === false) return "false";
  return String(value);
}

function looksLikeMaxLengthParam(name: string): boolean {
  return MAX_LENGTH_RE.test(name.toLowerCase());
}

function looksLikeLengthParam(name: string): boolean {
  return looksLikeMaxLengthParam(name) || MIN_LENGTH_RE.test(name.toLowerCase());
}

function looksLikeScaleParam(name: string): boolean {
  const lowered = name.toLowerCase();
  return NONNEG_INT_NAMES.has(lowered) || lowered.endsWith("_scale");
}

function looksLikePercentParam(name: string): boolean {
  return PERCENT_RE.test(name.toLowerCase());
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
  // Beyond 2^53 a JS number can't hold the whole number exactly, so a long
  // run of digits would silently be saved as a rounded value.
  if (!Number.isSafeInteger(number)) {
    throw new Error(`${name} is too large.`);
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
    // A maximum length of 0 matches nothing, but a minimum length of 0 is valid.
    if (looksLikeLengthParam(param.name)) return asInt(param.name, raw, looksLikeMaxLengthParam(param.name) ? 1 : 0);
    if (looksLikeScaleParam(param.name)) return asInt(param.name, raw, 0);
    if (param.type === "integer") return asInt(param.name, raw, null);
    const number = asNumber(param.name, raw);
    if (looksLikePercentParam(param.name) && (number < 0 || number > 100)) {
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
  return looksLikeLengthParam(param.name) || looksLikeScaleParam(param.name);
}

export function validateGroupParameterValues(
  parameters: RuleParameter[],
  rawValues: Record<string, unknown> | undefined,
  bindAll = false,
  overridableNames?: readonly string[] | null,
): { values: Record<string, RuleParameterValue>; error: string | null } {
  if (parameters.length === 0) return { values: {}, error: null };
  const coerced: Record<string, RuleParameterValue> = {};
  try {
    for (const param of parameters) {
      const canOverride = overridableNames == null || overridableNames.includes(param.name);
      const raw = canOverride
        ? (rawValues?.[param.name] ?? (isUnboundParameterValue(param.value) ? undefined : param.value))
        : (isUnboundParameterValue(param.value) ? undefined : param.value);
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
  const tokens = lowered.split("_");
  if (looksLikeLengthParam(name) || looksLikeScaleParam(name)) return "integer";
  if (lowered === "limit" || lowered === "threshold" || lowered.endsWith("_threshold") || RANGE_BOUND_RE.test(lowered)) {
    return "decimal";
  }
  if (tokens.includes("date")) return "date";
  if (tokens[0] === "allowed") return "list";
  // regex / pattern are plain strings — no separate parameter type.
  return "string";
}

/** Parameter types an author can declare on a SQL rule, in picker order. */
export const SQL_PARAMETER_TYPES = ["string", "integer", "decimal", "date", "list", "boolean"] as const;

/** The Test-rule cell family that renders the right input for a parameter type. */
export function parameterInputFamily(type: RuleParameter["type"]): "numeric" | "temporal" | "boolean" | "text" {
  if (type === "number" || type === "integer" || type === "decimal") return "numeric";
  if (type === "date") return "temporal";
  if (type === "boolean") return "boolean";
  return "text";
}

/** Double backslashes as well as quotes, matching backend ``escape_sql_string_strict``. */
function escapeSqlString(text: string): string {
  return text.replaceAll("\\", "\\\\").replaceAll("'", "''");
}

function sqlNumber(value: unknown): string {
  const number = typeof value === "number" ? value : Number(value);
  return Number.isInteger(number) ? String(number) : String(number);
}

/**
 * Render a bound parameter as a SQL literal, matching backend ``sql_literal``.
 * `quoted` means the token sits inside a single-quoted literal; `wholeLiteral`
 * means it is the entire literal (a list then becomes separately quoted items).
 */
export function sqlLiteral(
  param: Pick<RuleParameter, "name" | "type">,
  value: unknown,
  quoted: boolean,
  wholeLiteral = false,
): string {
  if (param.type === "boolean" || typeof value === "boolean") {
    const flag = typeof value === "boolean" ? value : String(value).trim().toLowerCase() === "true";
    if (quoted) return flag ? "true" : "false";
    return flag ? "TRUE" : "FALSE";
  }
  if (param.type === "number" || param.type === "integer" || param.type === "decimal") {
    return sqlNumber(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) => escapeSqlString(String(item)));
    if (!quoted) return items.map((item) => `'${item}'`).join(", ");
    return items.join(wholeLiteral ? "', '" : ", ");
  }
  const text = escapeSqlString(String(value));
  if (quoted) return text;
  if (param.type === "date") return `DATE'${text}'`;
  return `'${text}'`;
}

const SQL_TOKEN_RE = /\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/y;
const SQL_TOKEN_GLOBAL_RE = /\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/g;

/** Index just past the quoted region opening at `start` (doubled / backslash escapes honoured). */
function skipSqlRegion(text: string, start: number, delimiter: string): number {
  let i = start + 1;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "\\" && delimiter !== "`") {
      i += 2;
      continue;
    }
    if (ch === delimiter) {
      if (text[i + 1] === delimiter) {
        i += 2;
        continue;
      }
      return i + 1;
    }
    i += 1;
  }
  return text.length;
}

function substituteInRegion(region: string, delimiter: string, bound: Map<string, RuleParameter>): string {
  return region.replace(SQL_TOKEN_GLOBAL_RE, (match: string, name: string, offset: number) => {
    const param = bound.get(name);
    if (!param) return match;
    if (delimiter !== "'") {
      throw new Error(
        `${param.name} cannot be used inside a double-quoted string or backticked identifier; ` +
          "use a single-quoted literal or a bare token instead.",
      );
    }
    const whole = offset === 1 && offset + match.length === region.length - 1 && region.endsWith("'");
    return sqlLiteral(param, param.value, true, whole);
  });
}

/**
 * Replace bound ``{{param}}`` tokens in SQL text with typed literals, matching backend
 * ``substitute_sql_parameters``: the text is scanned so bare tokens become full literals
 * and tokens inside a single-quoted literal (also a longer one such as ``'^{{p}}.*'``)
 * are escaped for it. Comments are left untouched; tokens in a double-quoted string or
 * backticked identifier throw.
 */
export function substituteSqlParameters(text: string, parameters: RuleParameter[]): string {
  const bound = new Map(parameters.filter((param) => !isUnboundParameterValue(param.value)).map((p) => [p.name, p]));
  if (bound.size === 0) return text;
  let out = "";
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (text.startsWith("--", i)) {
      const newline = text.indexOf("\n", i);
      const end = newline === -1 ? text.length : newline;
      out += text.slice(i, end);
      i = end;
      continue;
    }
    if (text.startsWith("/*", i)) {
      const close = text.indexOf("*/", i + 2);
      const end = close === -1 ? text.length : close + 2;
      out += text.slice(i, end);
      i = end;
      continue;
    }
    if (ch === "'" || ch === '"' || ch === "`") {
      const end = skipSqlRegion(text, i, ch);
      out += substituteInRegion(text.slice(i, end), ch, bound);
      i = end;
      continue;
    }
    SQL_TOKEN_RE.lastIndex = i;
    const match = SQL_TOKEN_RE.exec(text);
    const param = match ? bound.get(match[1]) : undefined;
    if (match && param) {
      out += sqlLiteral(param, param.value, false);
      i += match[0].length;
      continue;
    }
    out += ch;
    i += 1;
  }
  return out;
}

/** Replace ``{{param}}`` tokens inside native-argument trees with JSON scalars. */
export function substituteParamPlaceholders(value: unknown, values: Record<string, unknown>): unknown {
  if (Array.isArray(value)) return value.map((item) => substituteParamPlaceholders(item, values));
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key,
        substituteParamPlaceholders(item, values),
      ]),
    );
  }
  if (typeof value !== "string") return value;
  const match = PLACEHOLDER_RE.exec(value.trim());
  if (match && match[1] in values) return values[match[1]];
  let result = value;
  for (const [name, replacement] of Object.entries(values)) {
    const token = `{{${name}}}`;
    if (result.includes(token)) result = result.replaceAll(token, String(replacement));
  }
  return result;
}
