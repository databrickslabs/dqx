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
// Date formatting
// ---------------------------------------------------------------------------

export function formatDateShort(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "—";
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
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "—";
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
