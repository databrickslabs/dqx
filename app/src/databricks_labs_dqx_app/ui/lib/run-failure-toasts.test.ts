import { describe, expect, test } from "bun:test";
import {
  runFailureTableLabel,
  selectFailureToasts,
  type RunFailureCandidate,
} from "./run-failure-toasts";

function run(
  partial: Partial<RunFailureCandidate> & { run_id: string; status: string | null },
): RunFailureCandidate {
  const kind = partial.kind ?? "validation";
  return {
    key: partial.key ?? `${kind}:${partial.run_id}`,
    run_id: partial.run_id,
    kind,
    status: partial.status,
    source_table_fqn: partial.source_table_fqn ?? "main.sales.orders",
  };
}

describe("selectFailureToasts — seeding pass (load-time storm guard)", () => {
  test("seeds every non-RUNNING run and toasts nothing", () => {
    const runs = [
      run({ run_id: "a", status: "FAILED" }),
      run({ run_id: "b", status: "SUCCESS" }),
      run({ run_id: "c", status: "RUNNING" }),
    ];
    const { toToast, seen } = selectFailureToasts(runs, new Set(), false);
    expect(toToast).toEqual([]);
    // FAILED + SUCCESS seeded; the RUNNING one is left unseen so its later
    // transition can be caught.
    expect(seen.has("validation:a")).toBe(true);
    expect(seen.has("validation:b")).toBe(true);
    expect(seen.has("validation:c")).toBe(false);
  });
});

describe("selectFailureToasts — steady state", () => {
  test("toasts a run that transitions RUNNING → FAILED after seeding", () => {
    // Seed with the run still RUNNING.
    const seeded = selectFailureToasts([run({ run_id: "c", status: "RUNNING" })], new Set(), false);
    expect(seeded.toToast).toEqual([]);

    // Next poll: it has failed.
    const { toToast, seen } = selectFailureToasts(
      [run({ run_id: "c", status: "FAILED" })],
      seeded.seen,
      true,
    );
    expect(toToast.map((r) => r.run_id)).toEqual(["c"]);
    expect(seen.has("validation:c")).toBe(true);
  });

  test("toasts a newly-appeared FAILED run not in the seed set", () => {
    const { toToast } = selectFailureToasts(
      [run({ run_id: "new", status: "FAILED" })],
      new Set(),
      true,
    );
    expect(toToast.map((r) => r.run_id)).toEqual(["new"]);
  });

  test("never re-toasts the same run on re-poll (dedup)", () => {
    const first = selectFailureToasts([run({ run_id: "c", status: "FAILED" })], new Set(), true);
    expect(first.toToast).toHaveLength(1);
    // Same failed run appears again on the next poll.
    const second = selectFailureToasts(
      [run({ run_id: "c", status: "FAILED" })],
      first.seen,
      true,
    );
    expect(second.toToast).toEqual([]);
  });

  test("does not toast SUCCESS/RUNNING/CANCELED runs", () => {
    const { toToast } = selectFailureToasts(
      [
        run({ run_id: "a", status: "SUCCESS" }),
        run({ run_id: "b", status: "RUNNING" }),
        run({ run_id: "d", status: "CANCELED" }),
      ],
      new Set(),
      true,
    );
    expect(toToast).toEqual([]);
  });

  test("validation and profiling runs with the same run_id are distinct", () => {
    const runs = [
      run({ run_id: "x", kind: "validation", status: "FAILED" }),
      run({ run_id: "x", kind: "profiling", status: "FAILED" }),
    ];
    const { toToast } = selectFailureToasts(runs, new Set(), true);
    expect(toToast.map((r) => r.key).sort()).toEqual(["profiling:x", "validation:x"]);
  });

  test("does not mutate the input seen set", () => {
    const seen = new Set<string>();
    selectFailureToasts([run({ run_id: "a", status: "FAILED" })], seen, true);
    expect(seen.size).toBe(0);
  });
});

describe("runFailureTableLabel", () => {
  test("returns the last dotted segment of a real FQN", () => {
    expect(runFailureTableLabel("main.sales.orders")).toBe("orders");
  });

  test("cleans the synthetic cross-table SQL prefix", () => {
    expect(runFailureTableLabel("__sql_check__/my_join_check")).toBe("my_join_check");
  });

  test("falls back to the whole string when there are no dots", () => {
    expect(runFailureTableLabel("orders")).toBe("orders");
  });
});
