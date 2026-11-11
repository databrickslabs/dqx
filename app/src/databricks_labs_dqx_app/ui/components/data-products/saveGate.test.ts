import { describe, expect, test } from "bun:test";
import { createSaveGate } from "./saveGate";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => {};
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("createSaveGate", () => {
  test("rejects a second action while the first is still running", async () => {
    const gate = createSaveGate();
    const first = deferred();
    const calls: string[] = [];

    const running = gate.run(async () => {
      calls.push("save");
      await first.promise;
      return "saved";
    });
    const second = await gate.run(async () => {
      calls.push("submit");
      return "submitted";
    });

    expect(second).toBeNull();
    expect(gate.busy).toBe(true);
    first.resolve();
    expect(await running).toBe("saved");
    expect(calls).toEqual(["save"]);
  });

  test("reopens once the running action finishes", async () => {
    const gate = createSaveGate();
    await gate.run(async () => "saved");

    expect(gate.busy).toBe(false);
    expect(await gate.run(async () => "submitted")).toBe("submitted");
  });

  test("reopens when the running action throws", async () => {
    const gate = createSaveGate();

    await expect(
      gate.run(async () => {
        throw new Error("save failed");
      }),
    ).rejects.toThrow("save failed");
    expect(gate.busy).toBe(false);
  });
});
