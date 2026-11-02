import { describe, expect, test } from "bun:test";

import { SetupGrantFailureErrorCode, type SetupApplyGrantsResponse } from "./api";
import en from "./i18n/locales/en.json";
import es from "./i18n/locales/es.json";
import fr from "./i18n/locales/fr.json";
import it from "./i18n/locales/it.json";
import ptBR from "./i18n/locales/pt-BR.json";
import {
  grantErrorMessageKey,
  grantRequestFailure,
  grantRunOutcome,
  grantStatementsSql,
} from "./setup-grants";

const FIRST = "GRANT USE_CATALOG ON CATALOG `main` TO `runner`;";
const SECOND = "GRANT USE_SCHEMA ON SCHEMA `main`.`dqx_studio` TO `runner`;";
const THIRD = "GRANT READ_VOLUME ON VOLUME `main`.`dqx_studio`.`wheels` TO `runner`;";

function response(
  applied: number,
  total: number,
  failed: SetupApplyGrantsResponse["failed"] = [],
): SetupApplyGrantsResponse {
  return {
    report: { state: "setup_required", steps: [] },
    applied,
    total,
    failed,
  };
}

describe("grantStatementsSql", () => {
  test("joins every statement on its own line", () => {
    expect(grantStatementsSql([FIRST, SECOND, THIRD])).toBe(
      `${FIRST}\n${SECOND}\n${THIRD}`,
    );
  });
});

describe("grantRunOutcome", () => {
  test("has nothing to show when every statement ran", () => {
    expect(grantRunOutcome(response(3, 3))).toBeNull();
  });

  test("has nothing to show when nothing was left to run, so the refreshed step shows", () => {
    expect(grantRunOutcome(response(0, 0))).toBeNull();
  });

  test("lists only the failed statements with a summary of the rest", () => {
    const outcome = grantRunOutcome(
      response(2, 3, [{ statement: SECOND, error_code: "permission_denied" }]),
    );

    expect(outcome).toEqual({
      summary: { applied: 2, total: 3 },
      failedSql: SECOND,
      errorCodes: ["permission_denied"],
    });
  });

  test("skips the summary when nothing ran", () => {
    const outcome = grantRunOutcome(
      response(0, 2, [
        { statement: FIRST, error_code: "warehouse_unavailable" },
        { statement: SECOND, error_code: "warehouse_unavailable" },
      ]),
    );

    expect(outcome?.summary).toBeNull();
    expect(outcome?.failedSql).toBe(`${FIRST}\n${SECOND}`);
    expect(outcome?.errorCodes).toEqual(["warehouse_unavailable"]);
  });

  test("names each distinct reason once, in order", () => {
    const outcome = grantRunOutcome(
      response(0, 3, [
        { statement: FIRST, error_code: "not_found" },
        { statement: SECOND, error_code: "permission_denied" },
        { statement: THIRD, error_code: "not_found" },
      ]),
    );

    expect(outcome?.errorCodes).toEqual(["not_found", "permission_denied"]);
  });
});

describe("grantRequestFailure", () => {
  test("asks for a fresh check when the list was out of date", () => {
    expect(grantRequestFailure("nothing_to_apply")).toEqual({
      summary: null,
      failedSql: "",
      errorCodes: ["not_found"],
    });
  });

  test("explains a run Studio couldn't finish or check", () => {
    const outcome = grantRequestFailure("setup_grants_failed");

    expect(outcome.errorCodes).toEqual(["setup_grants_failed"]);
    expect(grantErrorMessageKey("setup_grants_failed")).toBe(
      "setup.grants.errors.setup_grants_failed",
    );
  });

  test.each([
    ["en", en],
    ["fr", fr],
    ["es", es],
    ["it", it],
    ["pt-BR", ptBR],
  ] as const)("has a message for an unfinished run in %s", (code, locale) => {
    const message = locale.setup.grants.errors.setup_grants_failed;

    expect(message.length).toBeGreaterThan(0);
    if (code !== "en") expect(message).not.toBe(en.setup.grants.errors.setup_grants_failed);
  });

  test.each([undefined, "something_else"])(
    "falls back to the general message (%p)",
    (code) => {
      expect(grantRequestFailure(code).errorCodes).toEqual(["unknown"]);
    },
  );
});

describe("grantErrorMessageKey", () => {
  test.each([
    "permission_denied",
    "warehouse_unavailable",
    "not_found",
    "unknown",
  ] as const)("has a message key for %s", (code) => {
    expect(grantErrorMessageKey(code)).toBe(`setup.grants.errors.${code}`);
  });
});

describe("a rate-limited GRANT", () => {
  const busy = "Databricks is busy right now. Wait a moment, then try again.";

  test("has a plain message in setup and in schedules", () => {
    expect(SetupGrantFailureErrorCode.rate_limited).toBe("rate_limited");
    expect(en.setup.grants.errors.rate_limited).toBe(busy);
    expect(en.schedule.grantWarning.apply.errors.rate_limited).toBe(busy);
  });

  test.each([
    ["fr", fr],
    ["es", es],
    ["it", it],
    ["pt-BR", ptBR],
  ] as const)("is translated in %s", (_code, locale) => {
    for (const message of [
      locale.setup.grants.errors.rate_limited,
      locale.schedule.grantWarning.apply.errors.rate_limited,
    ]) {
      expect(message).toContain("Databricks");
      expect(message).not.toBe(busy);
    }
  });
});
