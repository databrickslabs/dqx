import { describe, expect, it } from "bun:test";
import { MutationObserver, QueryClient } from "@tanstack/react-query";
import { AxiosError, AxiosHeaders, type InternalAxiosRequestConfig } from "axios";
import {
  applyScheduleGrantsCallbacks,
  applyScheduleGrantsFailure,
  missingGrantTableFqns,
  scheduleGrantApplyResult,
  scheduleGrantPreflightQueryKey,
  scheduleGrantsRecheckFailure,
  splitPreflightTables,
} from "./useScheduleGrantPreflight";
import { getApplyScheduleGrantsMutationOptions, type SchedulePreflightTableOut } from "@/lib/api";

describe("splitPreflightTables", () => {
  it("separates denied tables from unverified ones", () => {
    const result = splitPreflightTables([
      { fqn: "c.s.ok", can_manage: true },
      {
        fqn: "c.s.denied",
        can_manage: false,
        manage_holders: [{ principal: "bob", type: "user" }],
      },
      { fqn: "c.s.unknown", can_manage: false, access_unverified: true },
    ]);
    expect(result.blockedTables.map((t) => t.fqn)).toEqual(["c.s.denied"]);
    expect(result.unverifiedTables.map((t) => t.fqn)).toEqual(["c.s.unknown"]);
    expect(result.hasGrantIssue).toBe(true);
  });

  it("an unverified table alone still blocks the save", () => {
    const result = splitPreflightTables([
      { fqn: "c.s.unknown", can_manage: false, access_unverified: true },
    ]);
    expect(result.blockedTables).toEqual([]);
    expect(result.hasGrantIssue).toBe(true);
  });

  it("reports missing scheduler grants separately from MANAGE denials", () => {
    const result = splitPreflightTables([
      {
        fqn: "c.s.parents",
        can_manage: false,
        missing_grants: [
          {
            principal: "runner-sp",
            display_name: "dqx-task-runner-sp",
            privilege: "USE_CATALOG",
            securable_type: "CATALOG",
            securable: "c",
            statement: "GRANT USE_CATALOG ON CATALOG `c` TO `runner-sp`;",
          },
        ],
      },
    ]);
    expect(result.missingGrantTables.map((t) => t.fqn)).toEqual(["c.s.parents"]);
    expect(result.blockedTables).toEqual([]);
    expect(result.hasGrantIssue).toBe(true);
  });

  it("no issue when every table is grantable", () => {
    expect(
      splitPreflightTables([{ fqn: "c.s.ok", can_manage: true }]).hasGrantIssue,
    ).toBe(false);
  });
});

const STATEMENT = "GRANT USE_CATALOG ON CATALOG `c` TO `runner-sp`;";

function missingGrantTable(fqn: string): SchedulePreflightTableOut {
  return {
    fqn,
    can_manage: false,
    missing_grants: [
      {
        principal: "runner-sp",
        display_name: "dqx-task-runner-sp",
        privilege: "USE_CATALOG",
        securable_type: "CATALOG",
        securable: "c",
        statement: STATEMENT,
      },
    ],
  };
}

describe("applying the missing scheduler grants", () => {
  it("sends only the tables with missing grants", () => {
    const { missingGrantTables } = splitPreflightTables([
      missingGrantTable("c.s.a"),
      { fqn: "c.s.ok", can_manage: true },
      { fqn: "c.s.unknown", can_manage: false, access_unverified: true },
      missingGrantTable("c.s.b"),
    ]);
    expect(missingGrantTableFqns(missingGrantTables)).toEqual(["c.s.a", "c.s.b"]);
  });

  it("the run icon posts the table names, shows the fresh check and refetches it", async () => {
    const queryClient = new QueryClient();
    const tableFqns = ["c.s.a", "c.s.ok"];
    const queryKey = scheduleGrantPreflightQueryKey(tableFqns);
    queryClient.setQueryData(queryKey, { tables: [missingGrantTable("c.s.a")] });
    const requests: { method?: string; url?: string; data?: unknown }[] = [];
    const fresh = { tables: [{ fqn: "c.s.a", can_manage: true }] };
    const mutation = new MutationObserver(
      queryClient,
      getApplyScheduleGrantsMutationOptions({
        mutation: applyScheduleGrantsCallbacks(queryClient, queryKey),
        axios: {
          adapter: async (config) => {
            requests.push({ method: config.method, url: config.url, data: config.data });
            return {
              data: { preflight: fresh, applied: 1, total: 1, failed: [] },
              status: 200,
              statusText: "OK",
              headers: {},
              config,
            };
          },
        },
      }),
    );

    await mutation.mutate({ data: { table_fqns: ["c.s.a"] } });

    expect(requests).toEqual([
      { method: "post", url: "/api/v1/schedule-grants/apply", data: JSON.stringify({ table_fqns: ["c.s.a"] }) },
    ]);
    expect(queryClient.getQueryData(queryKey)).toEqual(fresh);
    expect(queryClient.getQueryState(queryKey)?.isInvalidated).toBe(true);
  });

  it("a request that fails before anything runs gets a plain reason", () => {
    const failure = (status: number) =>
      applyScheduleGrantsFailure(
        new AxiosError("boom", "ERR", undefined, undefined, {
          status,
          statusText: "",
          data: {},
          headers: {},
          config: { headers: new AxiosHeaders() } as InternalAxiosRequestConfig,
        }),
      ).errorCodes;
    expect(failure(503)).toEqual(["warehouse_unavailable"]);
    expect(failure(500)).toEqual(["unknown"]);
    expect(applyScheduleGrantsFailure(new Error("network")).errorCodes).toEqual(["unknown"]);
  });
});

describe("a failed re-check after the grants ran", () => {
  it("keeps the counts so the card can say how many ran", () => {
    expect(
      scheduleGrantsRecheckFailure({
        preflight: { tables: [] },
        applied: 2,
        total: 3,
        failed: [],
        recheck_failed: true,
      }),
    ).toEqual({ applied: 2, total: 3 });
  });

  it("is null when the re-check ran", () => {
    expect(
      scheduleGrantsRecheckFailure({ preflight: { tables: [] }, applied: 1, total: 1, failed: [] }),
    ).toBeNull();
  });
});

function applyOptions(
  queryClient: QueryClient,
  queryKey: ReturnType<typeof scheduleGrantPreflightQueryKey>,
  respond: () => Promise<unknown>,
) {
  return getApplyScheduleGrantsMutationOptions({
    mutation: applyScheduleGrantsCallbacks(queryClient, queryKey),
    axios: {
      adapter: async (config) => ({
        data: await respond(),
        status: 200,
        statusText: "OK",
        headers: {},
        config,
      }),
    },
  });
}

function applyObserver(
  queryClient: QueryClient,
  queryKey: ReturnType<typeof scheduleGrantPreflightQueryKey>,
  respond: () => Promise<unknown>,
) {
  return new MutationObserver(queryClient, applyOptions(queryClient, queryKey, respond));
}

describe("the run's outcome belongs to the tables it ran for", () => {
  const failedRun = {
    preflight: { tables: [] },
    applied: 0,
    total: 1,
    failed: [{ fqn: "c.s.a", statement: STATEMENT, error_code: "permission_denied" as const }],
  };

  it("shows the outcome while the schedule's tables stay the same", () => {
    const result = scheduleGrantApplyResult(
      {
        isSuccess: true,
        isError: false,
        isPending: false,
        data: { data: failedRun },
        error: null,
        context: { queryKey: scheduleGrantPreflightQueryKey(["c.s.a", "c.s.b"]) },
      },
      ["c.s.a", "c.s.b"],
    );
    expect(result.applyOutcome?.failedSql).toBe(STATEMENT);
    expect(result.isApplying).toBe(false);
  });

  it("drops the outcome, and the failed SQL, once the schedule's tables change", () => {
    const apply = {
      isSuccess: true,
      isError: false,
      isPending: false,
      data: { data: { ...failedRun, recheck_failed: true } },
      error: null,
      context: { queryKey: scheduleGrantPreflightQueryKey(["c.s.a", "c.s.b"]) },
    };
    expect(scheduleGrantApplyResult(apply, ["c.s.a"])).toEqual({
      applyOutcome: null,
      recheckFailed: null,
      isApplying: false,
    });
  });

  it("doesn't show a run for other tables as still running", () => {
    const result = scheduleGrantApplyResult(
      {
        isSuccess: false,
        isError: false,
        isPending: true,
        data: undefined,
        error: null,
        context: { queryKey: scheduleGrantPreflightQueryKey(["c.s.a"]) },
      },
      ["c.s.b"],
    );
    expect(result.isApplying).toBe(false);
  });

  it("a run that finishes after the tables changed updates only its own tables' check", async () => {
    const queryClient = new QueryClient();
    const before = scheduleGrantPreflightQueryKey(["c.s.a"]);
    const after = scheduleGrantPreflightQueryKey(["c.s.b"]);
    const otherCheck = { tables: [{ fqn: "c.s.b", can_manage: true }] };
    queryClient.setQueryData(after, otherCheck);
    const fresh = { tables: [{ fqn: "c.s.a", can_manage: true }] };
    let finish: (value: unknown) => void = () => undefined;
    const respond = () => new Promise((resolve) => (finish = resolve));
    const observer = applyObserver(queryClient, before, respond);

    const running = observer.mutate({ data: { table_fqns: ["c.s.a"] } });
    // The schedule's tables change while the grants run: the hook re-renders with the new key.
    observer.setOptions(applyOptions(queryClient, after, respond));
    await new Promise((resolve) => setTimeout(resolve, 0));
    finish({ preflight: fresh, applied: 1, total: 1, failed: [] });
    await running;

    expect(queryClient.getQueryData(before)).toEqual(fresh);
    expect(queryClient.getQueryData(after)).toEqual(otherCheck);
  });
});

describe("a run whose re-check failed", () => {
  it("doesn't show the check from before the grants as current; it refetches instead", async () => {
    const queryClient = new QueryClient();
    const queryKey = scheduleGrantPreflightQueryKey(["c.s.a"]);
    const cached = { tables: [missingGrantTable("c.s.a")] };
    queryClient.setQueryData(queryKey, cached);
    const observer = applyObserver(queryClient, queryKey, async () => ({
      preflight: { tables: [missingGrantTable("c.s.a"), missingGrantTable("c.s.stale")] },
      applied: 1,
      total: 1,
      failed: [],
      recheck_failed: true,
    }));

    await observer.mutate({ data: { table_fqns: ["c.s.a"] } });

    expect(queryClient.getQueryData(queryKey)).toEqual(cached);
    expect(queryClient.getQueryState(queryKey)?.isInvalidated).toBe(true);
  });
});
