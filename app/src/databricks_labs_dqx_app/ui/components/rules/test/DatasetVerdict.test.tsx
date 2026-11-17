import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { DatasetVerdict } from "./DatasetVerdict";
import en from "@/lib/i18n/locales/en.json";
import es from "@/lib/i18n/locales/es.json";
import fr from "@/lib/i18n/locales/fr.json";
import it from "@/lib/i18n/locales/it.json";
import ptBR from "@/lib/i18n/locales/pt-BR.json";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

function render(props: Parameters<typeof DatasetVerdict>[0]): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <DatasetVerdict {...props} />
    </I18nextProvider>,
  );
}

describe("DatasetVerdict", () => {
  test("a failing dataset-level result is one verdict, not a one-cell grid", () => {
    const markup = render({ columns: ["condition"], rows: [{ cells: { condition: true }, passed: false }] });

    expect(markup).toContain(en.ruleTest.datasetVerdictFail);
    expect(markup).not.toContain("<table");
    expect(markup).not.toContain(">condition<");
  });

  test("a passing result lists the query's other values under the verdict", () => {
    const markup = render({
      columns: ["condition", "row_count"],
      rows: [{ cells: { condition: false, row_count: 1200 }, passed: true }],
    });

    expect(markup).toContain(en.ruleTest.datasetVerdictPass);
    expect(markup).toContain("row_count");
    expect(markup).toContain("1200");
  });

  test("more than one returned row is flagged, because the run rejects it", () => {
    const markup = render({
      columns: ["condition"],
      rows: [
        { cells: { condition: true }, passed: false },
        { cells: { condition: false }, passed: true },
      ],
    });

    expect(markup).toContain(en.ruleTest.datasetVerdictRowCount_other.replace("{{count}}", "2"));
  });

  test.each([
    ["fr", fr],
    ["es", es],
    ["it", it],
    ["pt-BR", ptBR],
  ] as const)("the verdict copy is translated in %s", (_code, locale) => {
    for (const key of ["datasetVerdictPass", "datasetVerdictFail", "datasetVerdictDescription"] as const) {
      expect(locale.ruleTest[key]).not.toBe(en.ruleTest[key]);
    }
  });
});
