import { describe, expect, test } from "bun:test";
import { createElement, createRef } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createInstance } from "i18next";
import { I18nextProvider, initReactI18next } from "react-i18next";
import { AllowedValuesEditor } from "./settings";
import { defToDraft } from "@/lib/label-definition-drafts";

const i18n = createInstance();
await i18n.use(initReactI18next).init({
  lng: "en",
  resources: { en: { translation: { config: { defaultValueBadge: "Default" } } } },
});

function renderValues(defaultValue: string | null): string {
  const draft = defToDraft({
    key: "team",
    values: ["value_1"],
    allow_custom_values: false,
    default_value: defaultValue,
  });
  return renderToStaticMarkup(
    createElement(
      I18nextProvider,
      { i18n },
      createElement(AllowedValuesEditor, {
        draft,
        onChange: () => {},
        outsideClickRootRef: createRef<HTMLDivElement>(),
      }),
    ),
  );
}

describe("AllowedValuesEditor — Default badge layout", () => {
  test("reserves badge space when the value is not the default", () => {
    expect(renderValues(null)).toContain("invisible");
  });

  test("shows the badge without hiding it for the default value", () => {
    expect(renderValues("value_1")).not.toContain("invisible");
  });
});
