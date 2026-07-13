/** Helpers for building the timezone picker's option list: each option carries
 *  its current UTC offset, and the list is ordered by that offset (most
 *  negative first). Offsets are DST-dependent, so we compute the *current*
 *  offset via Intl rather than hardcoding a static map that would rot. */

/** Fallback list used when Intl.supportedValuesOf is unavailable (older
 *  runtimes). Covers UTC plus the common zones the old hardcoded Select had. */
export const FALLBACK_TIMEZONES = [
  "UTC",
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Los_Angeles",
  "America/Sao_Paulo",
  "Europe/London",
  "Europe/Paris",
  "Europe/Berlin",
  "Europe/Madrid",
  "Asia/Kolkata",
  "Asia/Singapore",
  "Asia/Tokyo",
  "Australia/Sydney",
];

export interface TimezoneOption {
  /** IANA name, e.g. "America/New_York". This is the stored value. */
  value: string;
  /** Display label, e.g. "America/New_York (UTC-05:00)". */
  label: string;
  /** Current offset in minutes from UTC, used for sorting. */
  offsetMinutes: number;
}

/** Returns the current UTC offset for a timezone in minutes (e.g. -300 for
 *  America/New_York in winter, +330 for Asia/Kolkata). Returns 0 if the zone
 *  cannot be resolved. */
export function offsetMinutesFor(timeZone: string, now: Date = new Date()): number {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone,
      timeZoneName: "longOffset",
    }).formatToParts(now);
    const name = parts.find((p) => p.type === "timeZoneName")?.value;
    return name ? parseOffsetMinutes(name) : 0;
  } catch {
    return 0;
  }
}

/** Parses an Intl offset string like "GMT-05:00", "GMT+5:30", "UTC", or "GMT"
 *  into minutes. UTC/GMT with no offset → 0. */
export function parseOffsetMinutes(offsetName: string): number {
  const match = offsetName.match(/([+-])(\d{1,2})(?::?(\d{2}))?/);
  if (!match) return 0; // bare "GMT" / "UTC"
  const sign = match[1] === "-" ? -1 : 1;
  const hours = parseInt(match[2], 10);
  const minutes = match[3] ? parseInt(match[3], 10) : 0;
  return sign * (hours * 60 + minutes);
}

/** Formats an offset in minutes as "(UTC±HH:MM)", e.g. "(UTC-05:00)",
 *  "(UTC+05:30)", "(UTC+00:00)". */
export function formatOffset(offsetMinutes: number): string {
  const sign = offsetMinutes < 0 ? "-" : "+";
  const abs = Math.abs(offsetMinutes);
  const hours = String(Math.floor(abs / 60)).padStart(2, "0");
  const minutes = String(abs % 60).padStart(2, "0");
  return `(UTC${sign}${hours}:${minutes})`;
}

/** The raw IANA name list (UTC included), from Intl when available else the
 *  fallback. */
export function listTimeZones(): string[] {
  const supported =
    typeof Intl !== "undefined" &&
    typeof (Intl as { supportedValuesOf?: (k: string) => string[] })
      .supportedValuesOf === "function"
      ? (
          Intl as { supportedValuesOf: (k: string) => string[] }
        ).supportedValuesOf("timeZone")
      : null;
  const base = supported && supported.length > 0 ? supported : FALLBACK_TIMEZONES;
  // Dedupe in case UTC already appears in the supported list.
  return Array.from(new Set(["UTC", ...base]));
}

/** Builds picker options for the given zones (defaults to the full list),
 *  labelled with the current UTC offset and ordered by offset ascending (most
 *  negative → most positive). Ties broken alphabetically by name. */
export function buildTimezoneOptions(
  zones: string[] = listTimeZones(),
  now: Date = new Date(),
): TimezoneOption[] {
  return zones
    .map((value) => {
      const offsetMinutes = offsetMinutesFor(value, now);
      return {
        value,
        offsetMinutes,
        label: `${value} ${formatOffset(offsetMinutes)}`,
      };
    })
    .sort(
      (a, b) =>
        a.offsetMinutes - b.offsetMinutes || a.value.localeCompare(b.value),
    );
}
