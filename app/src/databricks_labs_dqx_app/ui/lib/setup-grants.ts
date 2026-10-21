import type { SetupGrantFailureErrorCode, SetupStepId } from "./api";

/** The result of running GRANT statements, as setup and schedule grants both return it. */
export type GrantRunResult = {
  applied: number;
  total: number;
  failed?: readonly {
    statement: string;
    error_code: SetupGrantFailureErrorCode;
  }[];
};

/**
 * Why GRANT statements didn't run: a statement's own failure reason, or
 * *setup_grants_failed* when Studio couldn't finish running a setup step's
 * statements or check setup again afterwards.
 */
export type GrantOutcomeCode = SetupGrantFailureErrorCode | "setup_grants_failed";

/** What to show under a step's GRANT statements after running them. */
export type GrantRunOutcome = {
  /** How many statements ran, shown only when some (but not all) did. */
  summary: { applied: number; total: number } | null;
  /** The statements that failed, one per line, for the administrator to copy. */
  failedSql: string;
  /** Each distinct failure reason once, in the order it first happened. */
  errorCodes: GrantOutcomeCode[];
};

/** A grant outcome tied to the step it belongs to. */
export type StepGrantOutcome = { stepId: SetupStepId; outcome: GrantRunOutcome };

/** All of a step's GRANT statements as one SQL block, one statement per line. */
export function grantStatementsSql(statements: readonly string[]): string {
  return statements.join("\n");
}

/** Describe a finished run, or null when every statement ran. */
export function grantRunOutcome(
  result: GrantRunResult,
): GrantRunOutcome | null {
  const failed = result.failed ?? [];
  if (failed.length === 0) return null;
  return {
    summary:
      result.applied > 0
        ? { applied: result.applied, total: result.total }
        : null,
    failedSql: grantStatementsSql(failed.map((failure) => failure.statement)),
    errorCodes: [...new Set(failed.map((failure) => failure.error_code))],
  };
}

/**
 * Describe a request that failed as a whole. A step with nothing left to run means
 * the list on screen was out of date, so the refresh message applies. A run Studio
 * couldn't finish or check may have given some access, so it asks for a fresh check.
 */
export function grantRequestFailure(code: string | undefined): GrantRunOutcome {
  let errorCode: GrantOutcomeCode = "unknown";
  if (code === "nothing_to_apply") errorCode = "not_found";
  else if (code === "setup_grants_failed") errorCode = "setup_grants_failed";
  return { summary: null, failedSql: "", errorCodes: [errorCode] };
}

/** Translation key for the message explaining a failure reason. */
export function grantErrorMessageKey(code: GrantOutcomeCode): string {
  return `setup.grants.errors.${code}`;
}
