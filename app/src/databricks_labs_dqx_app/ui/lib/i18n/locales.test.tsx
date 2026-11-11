import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider, Trans } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import en from "./locales/en.json";
import es from "./locales/es.json";
import fr from "./locales/fr.json";
import it from "./locales/it.json";
import ptBR from "./locales/pt-BR.json";

// Trans parses translations as HTML, so a tag named after a void element
// (e.g. <link>) is read as self-closing and its text never reaches the component.
// A lone void tag such as <br> is fine; wrapping text in one (<link>...</link>) is not.
const VOID_ELEMENTS = new Set([
  "area",
  "base",
  "br",
  "col",
  "command",
  "embed",
  "hr",
  "img",
  "input",
  "keygen",
  "link",
  "meta",
  "param",
  "source",
  "track",
  "wbr",
]);

const LOCALES = { en, es, fr, it, "pt-BR": ptBR } as const;

function strings(node: unknown, path: string): [string, string][] {
  if (typeof node === "string") return [[path, node]];
  if (node && typeof node === "object") {
    return Object.entries(node).flatMap(([key, value]) => strings(value, path ? `${path}.${key}` : key));
  }
  return [];
}

describe("locale Trans tags", () => {
  for (const [lng, bundle] of Object.entries(LOCALES)) {
    test(`${lng} wraps no text in a void-element tag`, () => {
      const offending = strings(bundle, "").flatMap(([path, value]) =>
        [...value.matchAll(/<\/([A-Za-z][\w-]*)\s*>/g)]
          .map((match) => match[1].toLowerCase())
          .filter((tag) => VOID_ELEMENTS.has(tag))
          .map((tag) => `${path}: </${tag}>`),
      );
      expect(offending).toEqual([]);
    });
  }

  test("the create-rule hint renders its link text inside the link", () => {
    const testI18n = i18next.createInstance();
    void testI18n.init({
      lng: "en",
      resources: { en: { translation: en } },
      interpolation: { escapeValue: false },
      initImmediate: false,
    });
    const html = renderToStaticMarkup(
      <I18nextProvider i18n={testI18n}>
        <Trans i18nKey="monitoredTables.createRuleInlineHint" components={{ createLink: <a href="/new" /> }} />
      </I18nextProvider>,
    );
    expect(html).toBe('Select an existing rule or <a href="/new">create a new one</a>.');
  });
});
