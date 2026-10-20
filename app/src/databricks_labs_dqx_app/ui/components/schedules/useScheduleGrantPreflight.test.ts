import { describe, expect, it } from "bun:test";
import { MutationObserver, QueryClient } from "@tanstack/react-query";
import { AxiosError, AxiosHeaders, type InternalAxiosRequestConfig } from "axios";
import {
  applyScheduleGrantsCallbacks,
  applyScheduleGrantsFailure,
  missingGrantTableFqns,
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
