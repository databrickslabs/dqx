import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { MappingChips } from "./MappingChips";
import type { RuleParameter, RuleSlot } from "@/lib/api";
import en from "@/lib/i18n/locales/en.json";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

const slots: RuleSlot[] = [{ name: "tier", family: "text", position: 0, cardinality: "one" }];
const parameters: RuleParameter[] = [
  // Filled and not unlocked → locked, hidden on the apply page.
  { name: "allowed", type: "list", value: ["Free", "Pro"] },
  // Empty → always overridable.
  { name: "limit", type: "integer", value: null },
  // Filled default the author unlocked → shown, pre-filled.
  { name: "note", type: "string", value: "vip" },
];
const overridable = ["limit", "note"];

function render(props: Partial<Parameters<typeof MappingChips>[0]>): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <MappingChips
        columnMapping={[]}
        slots={slots}
        parameters={parameters}
        overridableParameters={overridable}
        canEditParameters
        {...props}
      />
    </I18nextProvider>,
  );
}

describe("MappingChips apply-time parameters", () => {
  test("hides locked parameters and shows no padlock", () => {
    const html = render({ columnMapping: [{ tier: "account_tier" }] });
    expect(html).not.toContain("{{allowed}}");
    expect(html).toContain("{{limit}}");
    expect(html).toContain("{{note}}");
    expect(html).not.toContain(en.rulesRegistry.parametersPanelCannotOverrideTooltip);
    expect(html).not.toContain(en.rulesRegistry.parametersPanelCanOverrideTooltip);
  });

  test("pre-fills an unlocked parameter's default value", () => {
    const html = render({ columnMapping: [{ tier: "account_tier" }] });
    expect(html).toContain('value="vip"');
  });

  test("keeps the entry boxes when no column is mapped yet", () => {
    const html = render({ columnMapping: [], onAddGroup: () => undefined });
    expect(html).toContain(en.monitoredTables.noColumnMapped);
    expect(html).toContain('aria-label="limit"');
    expect(html).toContain('aria-label="note"');
  });

  test("keeps the entry boxes while the first column is being picked", () => {
    const html = render({
      columnMapping: [],
      onAddGroup: () => undefined,
      pendingValues: {},
      onPendingSelect: () => undefined,
    });
    expect(html).toContain('aria-label="limit"');
  });

  test("shows parameter boxes for a rule with no column slots", () => {
    const html = render({ slots: [], columnMapping: [] });
    expect(html).toContain('aria-label="limit"');
    expect(html).not.toContain("{{allowed}}");
  });
});
