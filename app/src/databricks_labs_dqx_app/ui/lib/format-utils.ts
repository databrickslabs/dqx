export function parseFqn(fqn: string) {
  const parts = fqn.split(".");
  return { catalog: parts[0] || "", schema: parts[1] || "", table: parts[2] || "" };
}

// ---------------------------------------------------------------------------
// Global display timezone (set from config, defaults to UTC)
// ---------------------------------------------------------------------------

let _displayTimezone = "UTC";

export function setDisplayTimezone(tz: string) {
  _displayTimezone = tz;
}

export function getDisplayTimezone(): string {
  return _displayTimezone;
}

// ---------------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------------

/**
 * Normalize a server timestamp string so JS parses the instant correctly.
 *
 * The analytical tables serialize timestamps via Spark ``CAST(ts AS STRING)``,
 * which yields a zone-less ``"YYYY-MM-DD HH:MM:SS"`` (space separator, no
 * ``Z``/offset). ``new Date()`` would read that as *browser-local* time, but
 * the warehouse clock is UTC — so a value gets shifted by the viewer's UTC
 * offset before the display-timezone projection, producing a wrong clock time.
 *
 * We pin any zone-less date-time to UTC (space → ``T``, append ``Z``). Strings
 * that already carry a timezone designator (``Z`` or ``±HH:MM``) pass through
 * untouched, as do values we can't confidently classify.
 */
function normalizeServerTimestamp(iso: string): string {
  const s = iso.trim();
  const hasZone = /([zZ]|[+-]\d{2}:?\d{2})$/.test(s);
  const isZonelessDateTime = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?$/.test(s);
  if (!hasZone && isZonelessDateTime) {
    return `${s.replace(" ", "T")}Z`;
  }
  return s;
}

/** Parse a server timestamp into a ``Date`` (zone-less values treated as UTC),
 *  or ``null`` when unparseable. */
export function parseServerDate(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(normalizeServerTimestamp(iso));
  return isNaN(d.getTime()) ? null : d;
}

/** Parse a server timestamp into epoch milliseconds (zone-less values treated
 *  as UTC), or ``NaN`` when unparseable. */
export function parseServerTimestampMs(iso: string | null | undefined): number {
  const d = parseServerDate(iso);
  return d ? d.getTime() : NaN;
}

// ---------------------------------------------------------------------------
// Date formatting
// ---------------------------------------------------------------------------

export function formatDateShort(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    const d = parseServerDate(iso);
    if (!d) return "—";
    const tz = _displayTimezone;
    return d.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
      timeZone: tz,
    }) + ` ${tz === "UTC" ? "UTC" : new Intl.DateTimeFormat(undefined, { timeZone: tz, timeZoneName: "short" }).formatToParts(d).find((p) => p.type === "timeZoneName")?.value ?? tz}`;
  } catch {
    return "—";
  }
}

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    const d = parseServerDate(iso);
    if (!d) return "—";
    const tz = _displayTimezone;
    return d.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: tz,
      timeZoneName: "short",
    });
  } catch {
    return "—";
  }
}

export type RelativeTimeParts =
  | { key: "justNow" }
  | { key: "minutesAgo" | "hoursAgo" | "daysAgo"; count: number };

/**
 * Relative time-since breakdown ("just now" / "5m ago" / "3h ago" / "2d
 * ago"), ported from dqlake's `BindingsTable` last-run/last-updated columns.
 * Callers translate `key` via i18next (e.g. `t("monitoredTables.relative." +
 * key, { count })`) and pair the rendered text with an in-app tooltip (not
 * the native `title` attribute) showing the absolute timestamp — see
 * `formatDateTime`.
 */
export function getRelativeTimeParts(iso: string | null | undefined): RelativeTimeParts | null {
  const d = parseServerDate(iso);
  if (!d) return null;
  const ms = Date.now() - d.getTime();
  const m = Math.floor(ms / 60000);
  if (m < 1) return { key: "justNow" };
  if (m < 60) return { key: "minutesAgo", count: m };
  if (m < 24 * 60) return { key: "hoursAgo", count: Math.floor(m / 60) };
  return { key: "daysAgo", count: Math.floor(m / (24 * 60)) };
}

export function formatUser(email: string | null | undefined): string {
  if (!email) return "—";
  const atIdx = email.indexOf("@");
  return atIdx > 0 ? email.substring(0, atIdx) : email;
}

// ---------------------------------------------------------------------------
// Label / user_metadata helpers
//
// Rule labels live in DQX's ``user_metadata`` field — a string→string map. The
// helpers below extract, format and tokenize labels so the rest of the UI can
// stay in sync without each page hand-rolling the same logic.
//
// The reserved key ``weight`` is treated like any other label by these helpers
// (storage is identical). Pages that care about weight semantics surface it
// distinctly, but it round-trips through user_metadata like the rest.
// ---------------------------------------------------------------------------

export const RESERVED_WEIGHT_KEY = "weight";

function coerceStringMap(input: Record<string, unknown>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(input)) {
    if (typeof v === "string") out[k] = v;
    else if (typeof v === "number" || typeof v === "boolean") out[k] = String(v);
    // Skip nested / null values — labels are flat string maps by contract.
  }
  return out;
}

/**
 * Best-effort extractor for the ``user_metadata`` map. Looks at common shapes
 * (top-level on a check, nested under ``check.user_metadata``, on a rule
 * catalog entry's first check, etc.) so callers can pass whatever shape they
 * have on hand without unwrapping it themselves.
 */
export function getUserMetadata(check: unknown): Record<string, string> {
  if (!check || typeof check !== "object") return {};
  const c = check as Record<string, unknown>;

  if (c.user_metadata && typeof c.user_metadata === "object" && !Array.isArray(c.user_metadata)) {
    return coerceStringMap(c.user_metadata as Record<string, unknown>);
  }
  if (c.userMetadata && typeof c.userMetadata === "object" && !Array.isArray(c.userMetadata)) {
    return coerceStringMap(c.userMetadata as Record<string, unknown>);
  }
  // Some shapes wrap the actual check under a `check` key.
  const inner = c.check as Record<string, unknown> | undefined;
  if (inner && typeof inner === "object") {
    if (inner.user_metadata && typeof inner.user_metadata === "object") {
      return coerceStringMap(inner.user_metadata as Record<string, unknown>);
    }
  }
  // Catalog entries have ``checks: [...]`` — fall back to the first one.
  const checks = c.checks as unknown;
  if (Array.isArray(checks) && checks.length > 0) {
    return getUserMetadata(checks[0]);
  }
  return {};
}

/** Render a label as ``key=value`` (or just ``key`` for boolean-style ``true``). */
export function formatLabel(key: string, value: string): string {
  if (value === "true" || value === "") return key;
  return `${key}=${value}`;
}

/** Stable string token for use in Sets / URL params, e.g. ``team\u0001finance``. */
export function labelToken(key: string, value: string): string {
  return `${key}\u0001${value}`;
}

export function tokenToLabel(token: string): { key: string; value: string } {
  const idx = token.indexOf("\u0001");
  if (idx < 0) return { key: token, value: "" };
  return { key: token.slice(0, idx), value: token.slice(idx + 1) };
}

/**
 * Split a Spark DDL string into top-level field definitions.
 *
 * We split on commas at bracket-depth 0 only, so nested types keep
 * their inner commas intact. Examples that must survive:
 *
 *   STRUCT<a: INT, b: STRING>
 *   ARRAY<STRING>
 *   MAP<STRING, INT>
 *   DECIMAL(10, 2)
 *
 * Pure string parsing, no Spark dependency on the client.
 */
export function splitDdlFields(ddl: string): string[] {
  const fields: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < ddl.length; i++) {
    const c = ddl[i];
    if (c === "<" || c === "(" || c === "[") depth++;
    else if (c === ">" || c === ")" || c === "]") depth = Math.max(0, depth - 1);
    else if (c === "," && depth === 0) {
      const chunk = ddl.slice(start, i).trim();
      if (chunk) fields.push(chunk);
      start = i + 1;
    }
  }
  const tail = ddl.slice(start).trim();
  if (tail) fields.push(tail);
  return fields;
}

/**
 * Extract the top-level field name from a single DDL field definition,
 * stripping backticks. Returns ``null`` for chunks that don't start
 * with a valid identifier (defensive — malformed input shouldn't
 * silently drop the whole filter).
 */
export function extractDdlFieldName(fieldDef: string): string | null {
  const trimmed = fieldDef.trimStart();
  const m = trimmed.match(/^(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)/);
  if (!m) return null;
  return m[1].replace(/^`|`$/g, "");
}

/**
 * Trim a Spark DDL string to a subset of its top-level columns.
 *
 * - ``mode = "include"``: keep only columns whose name (case-insensitive)
 *   is in *names*.
 * - ``mode = "exclude"``: drop columns whose name is in *names*; keep
 *   everything else.
 *
 * Returns the rewritten DDL preserving original ordering. Unparseable
 * chunks (no leading identifier) are passed through unchanged so we
 * don't silently mangle exotic input — caller can still validate.
 *
 * This is the workaround for the DQX ``has_valid_schema`` quirk where
 * the ``columns`` argument only filters the *actual* dataframe and
 * leaves the *expected* schema intact: trimming the expected schema
 * on this side keeps both sides of the comparison aligned.
 */
export function filterDdlByColumns(
  ddl: string,
  names: string[],
  mode: "include" | "exclude",
): string {
  if (names.length === 0) return ddl;
  const lookup = new Set(names.map((s) => s.toLowerCase()));
  const fields = splitDdlFields(ddl);
  const kept = fields.filter((def) => {
    const name = extractDdlFieldName(def);
    if (!name) {
      // Unparseable chunk — drop on include (we can't prove it matches)
      // and keep on exclude (it's not in our exclude list either way).
      return mode === "exclude";
    }
    const matched = lookup.has(name.toLowerCase());
    return mode === "include" ? matched : !matched;
  });
  return kept.join(", ");
}
