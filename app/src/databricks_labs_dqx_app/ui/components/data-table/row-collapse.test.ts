import { describe, expect, it } from "bun:test";
import { combinePhases, nextPhase, phaseOnToggle } from "./row-collapse";

describe("phaseOnToggle", () => {
  it("starts the enter / exit animation when animating", () => {
    expect(phaseOnToggle(true, true)).toBe("entering");
    expect(phaseOnToggle(false, true)).toBe("exiting");
  });

  it("jumps to the end state when the animation is skipped", () => {
    expect(phaseOnToggle(true, false)).toBe("open");
    expect(phaseOnToggle(false, false)).toBe("closed");
  });
});

describe("nextPhase", () => {
  it("walks entering -> expanding -> open", () => {
    expect(nextPhase("entering")).toBe("expanding");
    expect(nextPhase("expanding")).toBe("open");
    expect(nextPhase("open")).toBeNull();
  });

  it("unmounts after exiting", () => {
    expect(nextPhase("exiting")).toBe("closed");
    expect(nextPhase("closed")).toBeNull();
  });
});

describe("combinePhases", () => {
  it("follows the parent row while it animates", () => {
    expect(combinePhases("entering", "open")).toBe("entering");
    expect(combinePhases("exiting", "open")).toBe("exiting");
    expect(combinePhases("expanding", "open")).toBe("expanding");
  });

  it("uses its own phase once the parent is open", () => {
    expect(combinePhases("open", "exiting")).toBe("exiting");
    expect(combinePhases("open", "open")).toBe("open");
  });
});
