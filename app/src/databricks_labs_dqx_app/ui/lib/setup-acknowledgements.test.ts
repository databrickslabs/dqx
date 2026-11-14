import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import {
  acknowledgeWarnings,
  readAcknowledgedWarnings,
} from "./setup-acknowledgements";

let stored: Record<string, string>;
let original: PropertyDescriptor | undefined;

beforeEach(() => {
  stored = {};
  original = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: {
      getItem: (key: string) => stored[key] ?? null,
      setItem: (key: string, value: string) => {
        stored[key] = value;
      },
    },
  });
});

afterEach(() => {
  if (original) Object.defineProperty(globalThis, "localStorage", original);
  else Reflect.deleteProperty(globalThis, "localStorage");
});

describe("setup warning acknowledgements", () => {
  test("survive a reload", () => {
    acknowledgeWarnings(new Set(), ["app_sharing_unverified"]);

    expect(readAcknowledgedWarnings()).toEqual(new Set(["app_sharing_unverified"]));
  });

  test("accumulate with earlier acknowledgements", () => {
    const next = acknowledgeWarnings(new Set(["first"]), ["second"]);

    expect(next).toEqual(new Set(["first", "second"]));
    expect(readAcknowledgedWarnings()).toEqual(new Set(["first", "second"]));
  });

  test("ignore corrupt or unexpected stored values", () => {
    stored["dqx-setup-acknowledged-warnings"] = "{not json";
    expect(readAcknowledgedWarnings()).toEqual(new Set());

    stored["dqx-setup-acknowledged-warnings"] = JSON.stringify(["kept", 7, null]);
    expect(readAcknowledgedWarnings()).toEqual(new Set(["kept"]));
  });

  test("still acknowledge for the page when storage is unavailable", () => {
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: {
        getItem: () => {
          throw new Error("blocked");
        },
        setItem: () => {
          throw new Error("blocked");
        },
      },
    });

    expect(readAcknowledgedWarnings()).toEqual(new Set());
    expect(acknowledgeWarnings(new Set(), ["key"])).toEqual(new Set(["key"]));
  });
});
