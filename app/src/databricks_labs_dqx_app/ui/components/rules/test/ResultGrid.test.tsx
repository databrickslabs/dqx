import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { ResultGrid, ResultTable } from "./ResultGrid";
import { indexColumns, visibleColumns } from "./result-columns";
import en from "@/lib/i18n/locales/en.json";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

const COLUMNS = [
  { name: "id", mapped: true },
  { name: "amount", mapped: false },
  { name: "region", mapped: false },
];
const ROWS = [
  ["1", "10.5", "emea"],
  ["2", null, "apac"],
];

/** The text of every header cell, then of every body row's cells. */
function cells(markup: string): { headers: string[]; rows: string[][] } {
  const text = (html: string) => html.replace(/<[^>]+>/g, "");
  const headers = [...markup.matchAll(/<th[^>]*>(.*?)<\/th>/g)].map((m) => text(m[1]));
  const body = markup.slice(markup.indexOf("<tbody"));
  const rows = [...body.matchAll(/<tr[^>]*>(.*?)<\/tr>/g)].map((row) =>
    [...row[1].matchAll(/<td[^>]*>(.*?)<\/td>/g)].map((m) => text(m[1])),
  );
  return { headers, rows };
}

describe("ResultTable", () => {
  const indexed = indexColumns(COLUMNS);
  const order = indexed.map((c) => c.id);

  test("hiding a middle column keeps every remaining cell under its own header", () => {
    const shown = visibleColumns(indexed, order, new Set(["1"]));
    const { headers, rows } = cells(renderToStaticMarkup(<ResultTable columns={shown} rows={ROWS} />));
    expect(headers).toEqual(["id", "region"]);
    expect(rows).toEqual([
      ["1", "emea"],
      ["2", "apac"],
    ]);
  });

  test("a reordered column brings its own values with it", () => {
    const shown = visibleColumns(indexed, ["2", "0", "1"], new Set());
    const { headers, rows } = cells(renderToStaticMarkup(<ResultTable columns={shown} rows={ROWS} />));
    expect(headers).toEqual(["region", "id", "amount"]);
    expect(rows[1]).toEqual(["apac", "2", ""]);
  });

  test("rows are tinted by their verdict", () => {
    const markup = renderToStaticMarkup(<ResultTable columns={indexed} rows={ROWS} verdicts={[true, false]} />);
    const rowClasses = [...markup.slice(markup.indexOf("<tbody")).matchAll(/<tr class="([^"]*)"/g)].map((m) => m[1]);
    expect(rowClasses[0]).toContain("bg-green-500/12");
    expect(rowClasses[1]).toContain("bg-red-500/10");
  });

  test("the height cap can be shortened for a dialog", () => {
    expect(renderToStaticMarkup(<ResultTable columns={indexed} rows={ROWS} />)).toContain("max-h-[70vh]");
    const short = renderToStaticMarkup(<ResultTable columns={indexed} rows={ROWS} maxHeightClassName="max-h-[40vh]" />);
    expect(short).toContain("max-h-[40vh]");
    expect(short).not.toContain("max-h-[70vh]");
  });
});

describe("ResultGrid", () => {
  test("shows every column of a result, including two with the same name, with the Edit View menu", () => {
    const duplicated = [COLUMNS[0], COLUMNS[1], { name: "id", mapped: false }];
    const markup = renderToStaticMarkup(
      <I18nextProvider i18n={testI18n}>
        <ResultGrid columns={duplicated} rows={ROWS} />
      </I18nextProvider>,
    );
    const { headers, rows } = cells(markup);
    expect(headers).toEqual(["id", "amount", "id"]);
    expect(rows[0]).toEqual(["1", "10.5", "emea"]);
    expect(markup).toContain(en.common.editColumns);
  });
});
