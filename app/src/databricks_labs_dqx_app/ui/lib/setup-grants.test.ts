import { describe, expect, test } from "bun:test";

import type { SetupApplyGrantsResponse } from "./api";
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
