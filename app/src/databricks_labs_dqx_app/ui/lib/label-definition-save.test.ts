import { describe, expect, test } from "bun:test";
import { MutationObserver, QueryClient } from "@tanstack/react-query";
import { LABEL_DEFINITIONS_SAVE_SCOPE } from "./api-custom";

describe("label-definition save ordering", () => {
  test("serializes mutations in invocation order", async () => {
    const queryClient = new QueryClient();
    const started: string[] = [];
    let releaseFirst = () => {};
    const first = new MutationObserver(queryClient, {
      scope: LABEL_DEFINITIONS_SAVE_SCOPE,
      mutationFn: async () => {
        started.push("A");
        await new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
      },
    });
    const second = new MutationObserver(queryClient, {
      scope: LABEL_DEFINITIONS_SAVE_SCOPE,
      mutationFn: async () => {
        started.push("B");
      },
    });

    const firstResult = first.mutate(undefined);
    const secondResult = second.mutate(undefined);
    for (let i = 0; i < 10 && started.length === 0; i += 1) await Promise.resolve();
    expect(started).toEqual(["A"]);

    releaseFirst();
    await Promise.all([firstResult, secondResult]);
    expect(started).toEqual(["A", "B"]);
    queryClient.clear();
  });
});
