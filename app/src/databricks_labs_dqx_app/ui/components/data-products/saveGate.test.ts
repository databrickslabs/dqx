import { describe, expect, test } from "bun:test";
import { createSaveGate, runWhilePersisting, saveThenRefetch } from "./saveGate";

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

/** A Save / Submit wired the way useEditProductState wires them. */
function collectionSaves() {
  const gate = createSaveGate();
  const persisting: boolean[] = [];
  const events: string[] = [];
  const setPersisting = (value: boolean) => persisting.push(value);
  const start = (name: string, save: () => Promise<void>, refetch: () => Promise<void>) =>
    runWhilePersisting(gate, setPersisting, () =>
      saveThenRefetch({
        save: async () => {
          events.push(`${name}:save`);
          await save();
        },
        refetch: async () => {
          events.push(`${name}:refetch`);
          await refetch();
        },
        onSaved: () => events.push(`${name}:saved`),
        onFailed: () => events.push(`${name}:failed`),
      }),
    );
  return { start, persisting, events, isPersisting: () => persisting.at(-1) === true };
}

/** Let pending promise callbacks run. */
const settle = () => new Promise<void>((r) => setTimeout(r, 0));

describe("Save then Submit on a collection", () => {
  test("persisting stays set after the save lands, until its refetch resolves", async () => {
    const saves = collectionSaves();
    const refetch = deferred();

    const saving = saves.start("save", async () => {}, () => refetch.promise);
    await settle();

    // The save has been written and the refetch is in flight: still persisting.
    expect(saves.events).toEqual(["save:save", "save:saved", "save:refetch"]);
    expect(saves.isPersisting()).toBe(true);

    refetch.resolve();
    expect(await saving).toBe(true);
    expect(saves.persisting).toEqual([true, false]);
  });

  test("a Submit clicked while Save's refetch is in flight does not run", async () => {
    const saves = collectionSaves();
    const refetch = deferred();

    const saving = saves.start("save", async () => {}, () => refetch.promise);
    await settle();
    const submitted = await saves.start("submit", async () => {}, async () => {});

    expect(submitted).toBe(false);
    expect(saves.events.some((e) => e.startsWith("submit"))).toBe(false);
    refetch.resolve();
    await saving;
  });

  test("a Submit after Save's refetch has resolved runs normally", async () => {
    const saves = collectionSaves();

    await saves.start("save", async () => {}, async () => {});
    const submitted = await saves.start("submit", async () => {}, async () => {});

    expect(submitted).toBe(true);
    expect(saves.events).toEqual([
      "save:save",
      "save:saved",
      "save:refetch",
      "submit:save",
      "submit:saved",
      "submit:refetch",
    ]);
  });

  test("a failed save still refetches before releasing the gate", async () => {
    const saves = collectionSaves();
    const refetch = deferred();

    const saving = saves.start(
      "save",
      async () => {
        throw new Error("member not found");
      },
      () => refetch.promise,
    );
    await settle();

    expect(saves.events).toEqual(["save:save", "save:failed", "save:refetch"]);
    expect(saves.isPersisting()).toBe(true);
    refetch.resolve();
    expect(await saving).toBe(false);
    expect(saves.isPersisting()).toBe(false);
  });
});
