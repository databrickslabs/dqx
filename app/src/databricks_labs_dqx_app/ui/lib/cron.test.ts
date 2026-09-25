import { describe, expect, test } from "bun:test";
import type { TFunction } from "i18next";
import en from "./i18n/locales/en.json";
import { cronHint, cronToSimple, isValidCron, simpleToCron } from "./cron";

/** Minimal `t` backed by the real en.json bundle: resolves a dotted key to its
 *  template string and interpolates `{{name}}` placeholders. This keeps the
 *  humanizer assertions tied to the shipped English copy. */
type Node = string | { [k: string]: Node };

function template(key: string): string | undefined {
  let node: Node | undefined = en as Node;
  for (const part of key.split(".")) {
    if (typeof node !== "object" || node === null) return undefined;
    node = node[part];
  }
  return typeof node === "string" ? node : undefined;
}

const t = ((key: string, vars?: Record<string, unknown>): string => {
  const tmpl = template(key);
  if (tmpl === undefined) return key;
  return tmpl.replace(/\{\{(\w+)\}\}/g, (_match, name: string) => String(vars?.[name] ?? ""));
}) as unknown as TFunction;

describe("cronHint", () => {
  test("weekly on Sunday via dow 0", () => {
    expect(cronHint("0 3 * * 0", "UTC", t)).toBe("Runs weekly on Sunday at 03:00 (UTC)");
  });

  test("weekly on Sunday via dow 7", () => {
    expect(cronHint("0 3 * * 7", "UTC", t)).toBe("Runs weekly on Sunday at 03:00 (UTC)");
  });

  test("weekly on a mid-week day (numeric)", () => {
    expect(cronHint("30 9 * * 3", "UTC", t)).toBe("Runs weekly on Wednesday at 09:30 (UTC)");
  });

  test("weekly on a weekday named by its 3-letter token", () => {
    expect(cronHint("0 6 * * FRI", "UTC", t)).toBe("Runs weekly on Friday at 06:00 (UTC)");
  });

  test("weekly on Monday (the simple-picker shape) still names the day", () => {
    expect(cronHint("0 6 * * MON", "UTC", t)).toBe("Runs weekly on Monday at 06:00 (UTC)");
  });

  test("hourly", () => {
    expect(cronHint("15 * * * *", "UTC", t)).toBe("Runs hourly at minute 15 (UTC)");
  });

  test("daily", () => {
    expect(cronHint("0 6 * * *", "UTC", t)).toBe("Runs daily at 06:00 (UTC)");
  });

  test("timezone defaults to UTC when absent", () => {
    expect(cronHint("0 3 * * 0", null, t)).toBe("Runs weekly on Sunday at 03:00 (UTC)");
    expect(cronHint("0 6 * * *", "Europe/London", t)).toBe("Runs daily at 06:00 (Europe/London)");
  });

  test("multi-day dow ranges fall back to the raw hint", () => {
    expect(cronHint("0 3 * * 1-5", "UTC", t)).toBe("Runs on: 0 3 * * 1-5 (UTC)");
  });

  test("comma-listed weekdays fall back to the raw hint", () => {
    expect(cronHint("0 3 * * 1,3,5", "UTC", t)).toBe("Runs on: 0 3 * * 1,3,5 (UTC)");
  });

  test("unparseable expressions fall back to the raw hint", () => {
    expect(cronHint("not a cron", "UTC", t)).toBe("Runs on: not a cron (UTC)");
  });
});

describe("cronToSimple", () => {
  test("recognizes hourly / daily / weekly-Monday shapes", () => {
    expect(cronToSimple("15 * * * *")).toEqual({ cadence: "hourly", time: "00:15" });
    expect(cronToSimple("0 6 * * *")).toEqual({ cadence: "daily", time: "06:00" });
    expect(cronToSimple("0 6 * * MON")).toEqual({ cadence: "weekly", time: "06:00" });
  });

  test("returns null for weekdays the simple picker can't represent", () => {
    expect(cronToSimple("0 3 * * 0")).toBeNull();
    expect(cronToSimple("0 3 * * 3")).toBeNull();
  });

  test("round-trips with simpleToCron", () => {
    expect(cronToSimple(simpleToCron("daily", "06:00"))).toEqual({ cadence: "daily", time: "06:00" });
  });
});

describe("isValidCron", () => {
  test("accepts valid 5-field expressions including 0/7 Sunday", () => {
    expect(isValidCron("0 3 * * 0")).toBe(true);
    expect(isValidCron("0 3 * * 7")).toBe(true);
    expect(isValidCron("0 6 * * MON")).toBe(true);
  });

  test("rejects malformed expressions", () => {
    expect(isValidCron("not a cron")).toBe(false);
    expect(isValidCron("0 6 * *")).toBe(false);
  });
});
