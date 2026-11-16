import { useEffect, useState } from "react";

/** The timer functions a debounce needs; the globals by default, replaceable in tests. */
export interface DebounceTimers {
  setTimeout: (callback: () => void, delayMs: number) => unknown;
  clearTimeout: (id: unknown) => void;
}

const globalTimers: DebounceTimers = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (id) => globalThis.clearTimeout(id as ReturnType<typeof globalThis.setTimeout>),
};

/** Deliver *value* to *deliver* after *delayMs*, unless the returned cleanup runs first.
 *  Each new value's cleanup cancels the previous one's timer, so only a value left
 *  unchanged for *delayMs* is delivered; unmounting cancels whatever is pending. */
export function scheduleDebouncedValue<T>(
  value: T,
  delayMs: number,
  deliver: (value: T) => void,
  timers: DebounceTimers = globalTimers,
): () => void {
  const id = timers.setTimeout(() => deliver(value), delayMs);
  return () => timers.clearTimeout(id);
}

/** *value*, once it has stopped changing for *delayMs*. The first render returns it unchanged. */
export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => scheduleDebouncedValue(value, delayMs, setDebounced), [value, delayMs]);
  return debounced;
}
