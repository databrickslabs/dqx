import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { GroupByField, refToken } from "./GroupByField";
import en from "@/lib/i18n/locales/en.json";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

function render(value: string, names: string[]): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <GroupByField
        value={value}
        onChange={() => undefined}
        declaredColumns={names.map((name) => ({ name, family: "any" }))}
      />
    </I18nextProvider>,
  );
}

describe("GroupByField placeholder tokens", () => {
  test("trigger chip shows the {{placeholder}} token, not the bare name", () => {
    const html = render("{{column_1}}", ["column_1"]);
    expect(html).toContain("{{column_1}}");
  });

  test("dotted table.column refs stay unbraced", () => {
    const html = render("orders.id", ["orders.id"]);
    expect(html).toContain("orders.id");
    expect(html).not.toContain("{{orders.id}}");
  });

  // The popover rows render with the same helper; the popover is closed in
  // static markup, so pin the display contract here.
  test("option rows use refToken: braces for slots, none for dotted refs", () => {
    expect(refToken("column_1")).toBe("{{column_1}}");
    expect(refToken("orders.id")).toBe("orders.id");
  });
});
