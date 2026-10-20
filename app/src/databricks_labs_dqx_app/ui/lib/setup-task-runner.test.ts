import { describe, expect, test } from "bun:test";

import {
  accessResultMessageKeys,
  isApplicationId,
  isResultForPick,
  principalMatches,
  runAsErrorMessageKey,
  servicePrincipalLink,
  taskRunnerPrincipalList,
  taskRunnerSteps,
  type TaskRunnerAccessResult,
} from "./setup-task-runner";

const RUNNER = "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5";
const OTHER = "22222222-3333-4444-8555-666666666666";
const PRINCIPALS = [
  { id: "1", display_name: "dqx-studio-task-runner", application_id: RUNNER },
  { id: "2", display_name: "etl-runner", application_id: OTHER },
];

describe("isApplicationId", () => {
  test("accepts a dashed UUID in any case, with spaces around it", () => {
    expect(isApplicationId(RUNNER)).toBe(true);
    expect(isApplicationId(` ${RUNNER.toUpperCase()} `)).toBe(true);
  });

  test("rejects anything else", () => {
    for (const value of ["", "runner", "6f1c2d3e4a5b4c6d8e7f90a1b2c3d4e5", `${RUNNER}x`]) {
      expect(isApplicationId(value)).toBe(false);
    }
  });
});

describe("principalMatches", () => {
  test("lists everything without a search", () => {
    expect(principalMatches(PRINCIPALS, "").matches).toEqual(PRINCIPALS);
  });

  test("filters by name or Application ID, ignoring case", () => {
    expect(principalMatches(PRINCIPALS, "ETL").matches).toEqual([PRINCIPALS[1]]);
    expect(principalMatches(PRINCIPALS, RUNNER.slice(0, 8)).matches).toEqual([
      PRINCIPALS[0],
    ]);
  });

  test("offers a typed Application ID the list doesn't show", () => {
    const typed = "33333333-4444-4555-8666-777777777777";

    expect(principalMatches(PRINCIPALS, ` ${typed.toUpperCase()} `)).toEqual({
      matches: [],
      typedId: typed,
    });
  });

  test("doesn't offer a typed ID that is already listed, or text that isn't an ID", () => {
    expect(principalMatches(PRINCIPALS, RUNNER).typedId).toBeNull();
    expect(principalMatches(PRINCIPALS, "runner").typedId).toBeNull();
  });
});

describe("taskRunnerPrincipalList", () => {
  test("is loading while the list is pending", () => {
    expect(taskRunnerPrincipalList({ isPending: true, isError: false })).toEqual({
      status: "loading",
      appName: "",
    });
  });

  test("lists the service principals with the app's name", () => {
    expect(
      taskRunnerPrincipalList({
        isPending: false,
        isError: false,
        data: { available: true, principals: PRINCIPALS, app_name: "app-1a2b dqx-studio" },
      }),
    ).toEqual({ status: "ready", principals: PRINCIPALS, appName: "app-1a2b dqx-studio", truncated: false });
  });

  test("says when the list was cut short", () => {
    const list = taskRunnerPrincipalList({
      isPending: false,
      isError: false,
      data: { available: true, principals: PRINCIPALS, app_name: "dqx-studio", truncated: true },
    });

    expect(list).toMatchObject({ status: "ready", truncated: true });
  });

  test("falls back to pasting an ID when listing fails or finds nothing", () => {
    expect(taskRunnerPrincipalList({ isPending: false, isError: true })).toEqual({
      status: "unavailable",
      appName: "",
    });
    expect(
      taskRunnerPrincipalList({
        isPending: false,
        isError: false,
        data: { available: false, app_name: "dqx-studio" },
      }),
    ).toEqual({ status: "unavailable", appName: "dqx-studio" });
    expect(
      taskRunnerPrincipalList({
        isPending: false,
        isError: false,
        data: { available: true, principals: [], app_name: "dqx-studio" },
      }),
    ).toEqual({ status: "unavailable", appName: "dqx-studio" });
  });
});

describe("servicePrincipalLink", () => {
  test("links to the service principal's page when its ID is known", () => {
    expect(servicePrincipalLink("https://ws.example.com/", "42")).toBe(
      "https://ws.example.com/settings/workspace/identity-and-access/service-principals/42",
    );
  });

  test("links to the service principals list otherwise", () => {
    expect(servicePrincipalLink("https://ws.example.com")).toBe(
      "https://ws.example.com/settings/workspace/identity-and-access/service-principals",
    );
  });

  test("has no link without the workspace host", () => {
    expect(servicePrincipalLink(undefined, "42")).toBeNull();
    expect(servicePrincipalLink("  ")).toBeNull();
  });
});

function access(
  sp_permission: TaskRunnerAccessResult["sp_permission"],
  grant_statements: string[] = [],
  grants_unverified = false,
  automatic_grants: string[] = [],
): TaskRunnerAccessResult {
  return { sp_permission, grant_statements, automatic_grants, grants_unverified };
}

describe("accessResultMessageKeys", () => {
  test("confirms access when the app may use it and nothing is missing", () => {
    expect(accessResultMessageKeys(access("confirmed"))).toEqual([
      "setup.taskRunner.access.result.confirmed",
    ]);
  });

  test("explains a missing CAN USE, and an unverifiable one", () => {
    expect(accessResultMessageKeys(access("missing"))).toEqual([
      "setup.taskRunner.access.result.missing",
    ]);
    expect(accessResultMessageKeys(access("unverified"))).toEqual([
      "setup.taskRunner.access.result.unverified",
    ]);
  });

  test("adds the data access message when grants are pending", () => {
    expect(accessResultMessageKeys(access("confirmed", ["GRANT X"]))).toEqual([
      "setup.taskRunner.access.result.permissionConfirmed",
      "setup.taskRunner.access.result.grantsPending",
    ]);
    expect(accessResultMessageKeys(access("missing", ["GRANT X"]))).toEqual([
      "setup.taskRunner.access.result.missing",
      "setup.taskRunner.access.result.grantsPending",
    ]);
  });

  test("says Studio sets up the rest of the access itself once the job runs as it", () => {
    expect(accessResultMessageKeys(access("confirmed", [], false, ["GRANT Y"]))).toEqual([
      "setup.taskRunner.access.result.permissionConfirmed",
      "setup.taskRunner.access.result.automaticGrants",
    ]);
    expect(
      accessResultMessageKeys(access("unverified", ["GRANT X"], false, ["GRANT Y"])),
    ).toEqual([
      "setup.taskRunner.access.result.unverified",
      "setup.taskRunner.access.result.grantsPending",
      "setup.taskRunner.access.result.automaticGrants",
    ]);
  });

  test("doesn't count Studio's own grants as missing on step 3", () => {
    expect(
      taskRunnerSteps({
        selected: true,
        access: access("confirmed", [], false, ["GRANT Y"]),
      }),
    ).toMatchObject({ applyEnabled: true, grantsStillMissing: false });
  });

  test("says when some data access couldn't be checked", () => {
    expect(accessResultMessageKeys(access("confirmed", [], true))).toEqual([
      "setup.taskRunner.access.result.permissionConfirmed",
      "setup.taskRunner.access.result.grantsUnverified",
    ]);
  });
});

describe("taskRunnerSteps", () => {
  test("keeps steps 2 and 3 disabled until a service principal is picked", () => {
    expect(taskRunnerSteps({ selected: false, access: null })).toEqual({
      confirmEnabled: false,
      applyEnabled: false,
      applyHint: "pickFirst",
      grantsStillMissing: false,
    });
  });

  test("needs access to be confirmed once before applying", () => {
    expect(taskRunnerSteps({ selected: true, access: null })).toEqual({
      confirmEnabled: true,
      applyEnabled: false,
      applyHint: "confirmFirst",
      grantsStillMissing: false,
    });
  });

  test("a missing role is only a warning: step 3 stays enabled, and setting Run as proves it", () => {
    expect(taskRunnerSteps({ selected: true, access: access("missing") })).toEqual({
      confirmEnabled: true,
      applyEnabled: true,
      applyHint: null,
      grantsStillMissing: false,
    });
  });

  test("allows applying once access is confirmed or can't be checked", () => {
    for (const permission of ["confirmed", "unverified"] as const) {
      expect(taskRunnerSteps({ selected: true, access: access(permission) })).toEqual({
        confirmEnabled: true,
        applyEnabled: true,
        applyHint: null,
        grantsStillMissing: false,
      });
    }
  });

  test("allows applying with grants still missing, and says so", () => {
    expect(
      taskRunnerSteps({ selected: true, access: access("confirmed", ["GRANT X"]) }),
    ).toMatchObject({ applyEnabled: true, grantsStillMissing: true });
  });

  test("disables everything while busy", () => {
    expect(
      taskRunnerSteps({ selected: true, access: access("confirmed"), busy: true }),
    ).toMatchObject({ confirmEnabled: false, applyEnabled: false });
  });
});

describe("runAsErrorMessageKey", () => {
  test("has a message for each refusal, and a default", () => {
    expect(runAsErrorMessageKey("task_runner_run_as_forbidden")).toBe(
      "setup.taskRunner.apply.errors.task_runner_run_as_forbidden",
    );
    expect(runAsErrorMessageKey("task_runner_principal_not_found")).toBe(
      "setup.taskRunner.apply.errors.task_runner_principal_not_found",
    );
    expect(runAsErrorMessageKey("something_else")).toBe(
      "setup.taskRunner.apply.errors.default",
    );
    expect(runAsErrorMessageKey(undefined)).toBe("setup.taskRunner.apply.errors.default");
  });
});

describe("isResultForPick", () => {
  test("applies a result for the service principal still picked, ignoring case", () => {
    expect(
      isResultForPick({ applicationId: RUNNER, displayName: "runner" }, RUNNER.toUpperCase()),
    ).toBe(true);
  });

  test("ignores a result that arrives after another service principal was picked", () => {
    expect(isResultForPick({ applicationId: OTHER, displayName: "etl-runner" }, RUNNER)).toBe(false);
  });

  test("ignores a result once the pick is cleared", () => {
    expect(isResultForPick(null, RUNNER)).toBe(false);
  });
});
