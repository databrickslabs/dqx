import { describe, expect, test } from "bun:test";
import { defToDraft, draftToDef } from "./label-definition-drafts";
import type { LabelDefinition } from "./api-custom";

// Unit tests for the pure draft <-> API mapping helpers behind the admin
// Label Definitions editor (config page), focusing on the severity ->
// DQX criticality map added by the DQ Score work. Run via `bun test`
// (see `make app-test-ui`).

const def = (over: Partial<LabelDefinition> = {}): LabelDefinition => ({
  key: "severity",
  description: "",
  values: ["Low", "Medium", "High", "Critical"],
  allow_custom_values: false,
  value_colors: null,
  value_descriptions: null,
  value_criticality: { Low: "warn", Medium: "warn", High: "error", Critical: "error" },
  is_builtin: true,
  ...over,
});

describe("defToDraft / draftToDef — value_criticality", () => {
  test("round-trips the severity criticality map unchanged", () => {
    expect(draftToDef(defToDraft(def())).value_criticality).toEqual({
      Low: "warn",
      Medium: "warn",
      High: "error",
      Critical: "error",
    });
  });

  test("defToDraft copies the map so draft edits don't mutate the source", () => {
    const source = def();
    const draft = defToDraft(source);
    draft.value_criticality!.Critical = "warn";
    expect(source.value_criticality!.Critical).toBe("error");
  });

  test("draftToDef prunes criticality entries for removed values", () => {
    const draft = defToDraft(def());
    draft.values = ["Low", "High"];
    expect(draftToDef(draft).value_criticality).toEqual({ Low: "warn", High: "error" });
  });

  test("draftToDef drops entries that are neither warn nor error", () => {
    const draft = defToDraft(def({ value_criticality: { Low: "fatal", High: "error" } }));
    expect(draftToDef(draft).value_criticality).toEqual({ High: "error" });
  });

  test("draftToDef normalises an empty/missing map to null", () => {
    expect(draftToDef(defToDraft(def({ value_criticality: null }))).value_criticality).toBeNull();
    expect(draftToDef(defToDraft(def({ value_criticality: {} }))).value_criticality).toBeNull();
  });
});

describe("defToDraft / draftToDef — existing color/description behavior (moved from settings.tsx)", () => {
  test("prunes colors and descriptions for values not in the trimmed value list", () => {
    const draft = defToDraft(
      def({
        values: ["Low", "  ", "High"],
        value_colors: { Low: "#16A34A", Ghost: "#000000", High: "not-a-color" },
        value_descriptions: { Low: "minor", Ghost: "gone", High: "   " },
      }),
    );
    const out = draftToDef(draft);
    expect(out.values).toEqual(["Low", "High"]);
    expect(out.value_colors).toEqual({ Low: "#16A34A" });
    expect(out.value_descriptions).toEqual({ Low: "minor" });
  });
});

describe("defToDraft / draftToDef — required + default_value", () => {
  const team = (over: Partial<LabelDefinition> = {}): LabelDefinition =>
    def({ key: "team", values: ["a", "b"], value_criticality: null, is_builtin: false, ...over });

  test("round-trips required and default_value", () => {
    const out = draftToDef(defToDraft(team({ required: true, default_value: "b" })));
    expect(out.required).toBe(true);
    expect(out.default_value).toBe("b");
  });

  test("drops a default that is no longer an allowed value", () => {
    const draft = defToDraft(team({ default_value: "b" }));
    draft.values = ["a"];
    expect(draftToDef(draft).default_value).toBeNull();
  });

  test("never marks dimension or severity as required", () => {
    expect(draftToDef(defToDraft(def({ required: true }))).required).toBe(false);
  });

  test("defaults missing fields to not-required with no default", () => {
    const out = draftToDef(defToDraft(team()));
    expect(out.required).toBe(false);
    expect(out.default_value).toBeNull();
  });
});
