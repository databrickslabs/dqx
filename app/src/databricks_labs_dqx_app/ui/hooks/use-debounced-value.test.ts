import { describe, expect, test } from "bun:test";
import { type DebounceTimers, scheduleDebouncedValue } from "./use-debounced-value";

/** Timers driven by hand: *advance* runs every timer due by then, in order. */
function manualTimers(): DebounceTimers & { advance: (ms: number) => void; pending: () => number } {
  let now = 0;
  let nextId = 0;
  const timers = new Map<number, { at: number; callback: () => void }>();
  return {
    setTimeout: (callback, delayMs) => {
      nextId += 1;
      timers.set(nextId, { at: now + delayMs, callback });
      return nextId;
    },
    clearTimeout: (id) => {
      timers.delete(id as number);
    },
    advance: (ms) => {
      now += ms;
      const due = [...timers.entries()].filter(([, t]) => t.at <= now).sort(([, a], [, b]) => a.at - b.at);
      for (const [id, t] of due) {
        timers.delete(id);
        t.callback();
      }
    },
    pending: () => timers.size,
  };
}

/** Feed *values* through the debounce the way the hook's effect does: each new
 *  value runs the previous value's cleanup first. */
function typeValues(values: string[], gapMs: number, delayMs: number, timers: ReturnType<typeof manualTimers>) {
  const delivered: string[] = [];
  let cleanup: (() => void) | undefined;
  for (const value of values) {
    cleanup?.();
    cleanup = scheduleDebouncedValue(value, delayMs, (v) => delivered.push(v), timers);
    timers.advance(gapMs);
  }
  return { delivered, unmount: () => cleanup?.() };
}

describe("scheduleDebouncedValue", () => {
  test("delivers nothing while the value keeps changing faster than the delay", () => {
    const timers = manualTimers();

    const { delivered } = typeValues(["S", "SE", "SEL"], 100, 500, timers);

    expect(delivered).toEqual([]);
  });

  test("delivers only the last value once typing pauses for the delay", () => {
    const timers = manualTimers();

    const { delivered } = typeValues(["S", "SE", "SEL"], 100, 500, timers);
    timers.advance(400);

    expect(delivered).toEqual(["SEL"]);
  });

  test("unmounting before the delay cancels the pending value and leaves no timer behind", () => {
    const timers = manualTimers();

    const { delivered, unmount } = typeValues(["SELECT"], 100, 500, timers);
    unmount();
    timers.advance(1000);

    expect(delivered).toEqual([]);
    expect(timers.pending()).toBe(0);
  });
});
