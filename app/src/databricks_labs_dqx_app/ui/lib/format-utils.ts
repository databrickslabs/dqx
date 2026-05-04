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
