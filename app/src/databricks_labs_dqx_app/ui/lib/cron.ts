/** Shared helpers for the standard 5-field cron dialect the DQX in-app
 *  scheduler evaluates (`SchedulerService._compute_next_cron_run` in
 *  `backend/services/scheduler_service.py`).
 *
 *  Adapted from dqlake's `ui/lib/cron.ts`, which speaks 6/7-field Quartz
 *  cron (seconds first, `?` wildcard). Our backend evaluator is a plain
 *  POSIX 5-field cron — `minute hour day-of-month month day-of-week`, no
 *  seconds field, no `?` — so every helper here (`isValidCron`,
 *  `simpleToCron`, `cronToSimple`, `cronHint`) is a fresh implementation of
 *  the same *shape* of helper, re-derived from the backend's own field
 *  grammar (`SchedulerService._parse_cron_field`) rather than a port of the
 *  Quartz-specific logic. Keep this in sync with that grammar: a wildcard,
 *  comma-separated lists, `a-b` ranges, and step suffixes (wildcard-slash-n
 *  or range-slash-n) in every field; day-of-week additionally accepts
 *  case-insensitive `MON`..`SUN` names (`0`/`7` both mean Sunday, matching
 *  POSIX cron).
 */

import type { TFunction } from "i18next";

export type SimpleCadence = "hourly" | "daily" | "weekly";

/** Schedule entry mode: a friendly cadence picker, or a raw 5-field cron. */
export type ScheduleMode = "simple" | "custom";

/** Weekday name -> POSIX cron numeric value, mirroring the backend's
 *  `_CRON_WEEKDAY_NAMES` (0 = Sunday). Day-of-week is the only field that
 *  accepts names. */
const WEEKDAY_NAMES: Record<string, number> = {
  SUN: 0,
  MON: 1,
  TUE: 2,
  WED: 3,
  THU: 4,
  FRI: 5,
  SAT: 6,
};

/** The day-of-week token the simple "Week" cadence always schedules onto
 *  (matches dqlake's simple picker, which has no day-of-week selector). */
const WEEKLY_DOW = "MON";

function resolveToken(token: string, names?: Record<string, number>): number | null {
  const trimmed = token.trim();
  if (names) {
    const upper = trimmed.toUpperCase();
    if (upper in names) return names[upper];
  }
  if (!/^\d+$/.test(trimmed)) return null;
  return Number(trimmed);
}

/** Validate one cron field against the backend's `_parse_cron_field` grammar:
 *  comma-separated list of a wildcard, `N`, `A-B`, a stepped wildcard, or a
 *  stepped range, each resolving to a value (or range) within `[lo, hi]`. */
function isValidCronField(raw: string, lo: number, hi: number, names?: Record<string, number>): boolean {
  const parts = raw.trim().split(",");
  let sawValue = false;
  for (const partRaw of parts) {
    const part = partRaw.trim();
    if (!part) continue;
    const slash = part.indexOf("/");
    const base = slash === -1 ? part : part.slice(0, slash);
    const stepStr = slash === -1 ? undefined : part.slice(slash + 1);
    if (stepStr !== undefined) {
      if (!/^\d+$/.test(stepStr) || Number(stepStr) <= 0) return false;
    }
    let start: number | null;
    let end: number | null;
    if (base === "*") {
      start = lo;
      end = hi;
    } else if (base.includes("-")) {
      const dash = base.indexOf("-");
      start = resolveToken(base.slice(0, dash), names);
      end = resolveToken(base.slice(dash + 1), names);
    } else {
      start = resolveToken(base, names);
      end = start;
    }
    if (start === null || end === null) return false;
    if (start < lo || start > hi || end < lo || end > hi || start > end) return false;
    sawValue = true;
  }
  return sawValue;
}

/** True when *cron* is a valid standard 5-field cron expression the DQX
 *  scheduler will accept (minute hour day-of-month month day-of-week). */
export function isValidCron(cron: string): boolean {
  const fields = cron.trim().split(/\s+/).filter(Boolean);
  if (fields.length !== 5) return false;
  const [minute, hour, dom, month, dow] = fields;
  return (
    isValidCronField(minute, 0, 59) &&
    isValidCronField(hour, 0, 23) &&
    isValidCronField(dom, 1, 31) &&
    isValidCronField(month, 1, 12) &&
    isValidCronField(dow, 0, 7, WEEKDAY_NAMES)
  );
}

/** Compile a simple cadence + "HH:MM" time into the exact 5-field cron the
 *  backend evaluator accepts. Inverse of `cronToSimple` for these shapes. */
export function simpleToCron(cadence: SimpleCadence, time: string): string {
  const [rawHh, rawMm] = (time || "00:00").split(":");
  const hh = Number(rawHh) || 0;
  const mm = Number(rawMm) || 0;
  if (cadence === "hourly") return `${mm} * * * *`;
  if (cadence === "weekly") return `${mm} ${hh} * * ${WEEKLY_DOW}`;
  return `${mm} ${hh} * * *`;
}

/** Best-effort inverse of `simpleToCron` for the three cadences the simple
 *  picker supports. Returns null when the cron doesn't match one of those
 *  exact shapes, in which case the caller should fall back to Custom (raw
 *  cron) mode. */
export function cronToSimple(cron: string | null | undefined): { cadence: SimpleCadence; time: string } | null {
  if (!cron) return null;
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) return null;
  const [minute, hour, dom, month, dow] = parts;
  if (!/^\d+$/.test(minute)) return null;
  const mm = String(Number(minute)).padStart(2, "0");

  // Hourly: "{mm} * * * *".
  if (hour === "*" && dom === "*" && month === "*" && dow === "*") {
    return { cadence: "hourly", time: `00:${mm}` };
  }

  if (!/^\d+$/.test(hour)) return null;
  const hh = String(Number(hour)).padStart(2, "0");
  const time = `${hh}:${mm}`;

  // Weekly: "{mm} {hh} * * MON".
  if (dom === "*" && month === "*" && dow.toUpperCase() === WEEKLY_DOW) {
    return { cadence: "weekly", time };
  }

  // Daily: "{mm} {hh} * * *".
  if (dom === "*" && month === "*" && dow === "*") {
    return { cadence: "daily", time };
  }

  return null;
}

/** Human-readable hint for a cron expression, built entirely from `t()`
 *  fragments (i18n-sane by construction — no English string literals are
 *  interpolated into the result). Falls back to a generic "runs on: <cron>"
 *  translation for shapes outside the three simple cadences. Callers should
 *  gate on `isValidCron` first for malformed input. */
export function cronHint(cron: string, tz: string | null | undefined, t: TFunction): string {
  const trimmed = cron.trim();
  const tzLabel = tz || "UTC";
  const simple = cronToSimple(trimmed);
  if (simple) {
    if (simple.cadence === "hourly") {
      const minute = simple.time.split(":")[1];
      return t("dataProducts.scheduleHintHourly", { minute, tz: tzLabel });
    }
    if (simple.cadence === "weekly") {
      return t("dataProducts.scheduleHintWeekly", { time: simple.time, tz: tzLabel });
    }
    return t("dataProducts.scheduleHintDaily", { time: simple.time, tz: tzLabel });
  }
  return t("dataProducts.scheduleHintRaw", { cron: trimmed, tz: tzLabel });
}
