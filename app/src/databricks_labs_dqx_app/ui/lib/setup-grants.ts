import type {
  SetupApplyGrantsResponse,
  SetupGrantFailureErrorCode,
  SetupStepId,
} from "./api";

/** What to show under a step's GRANT statements after running them. */
export type GrantRunOutcome = {
  /** How many statements ran, shown only when some (but not all) did. */
  summary: { applied: number; total: number } | null;
  /** The statements that failed, one per line, for the administrator to copy. */
  failedSql: string;
  /** Each distinct failure reason once, in the order it first happened. */
  errorCodes: SetupGrantFailureErrorCode[];
};

/** A grant outcome tied to the step it belongs to. */
export type StepGrantOutcome = { stepId: SetupStepId; outcome: GrantRunOutcome };

/** All of a step's GRANT statements as one SQL block, one statement per line. */
export function grantStatementsSql(statements: readonly string[]): string {
  return statements.join("\n");
}

/** Describe a finished run, or null when every statement ran. */
export function grantRunOutcome(
  result: SetupApplyGrantsResponse,
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
 * Describe a request that failed before any statement ran. A step with nothing left
 * to run means the list on screen was out of date, so the refresh message applies.
 */
export function grantRequestFailure(code: string | undefined): GrantRunOutcome {
  return {
    summary: null,
    failedSql: "",
    errorCodes: [code === "nothing_to_apply" ? "not_found" : "unknown"],
  };
}

/** Translation key for the message explaining a failure reason. */
export function grantErrorMessageKey(code: SetupGrantFailureErrorCode): string {
  return `setup.grants.errors.${code}`;
}
