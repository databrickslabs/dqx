import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { SetupConfigurationForm } from "./SetupConfigurationForm";
import en from "@/lib/i18n/locales/en.json";
import type { SetupCatalogList } from "@/lib/setup-catalogs";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

function renderForm(
  props: Partial<React.ComponentProps<typeof SetupConfigurationForm>> = {},
): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <SetupConfigurationForm
        isSubmitting={false}
        onSubmit={() => undefined}
        {...props}
      />
    </I18nextProvider>,
  );
}

function escapeHtml(text: string): string {
  return renderToStaticMarkup(<>{text}</>);
}

const READY: SetupCatalogList = { status: "ready", catalogs: ["main", "sales"] };

describe("catalog field", () => {
  test("is a searchable picker labelled by the catalog label when catalogs are listed", () => {
    const html = renderForm({ catalogList: READY });

    expect(html).toMatch(/<button[^>]*id="setup-catalog"[^>]*role="combobox"|<button[^>]*role="combobox"[^>]*id="setup-catalog"/);
    expect(html).toContain('for="setup-catalog"');
    expect(html).not.toContain('name="catalog"');
    expect(html).toContain(en.setup.configuration.catalogPlaceholder);
  });

  test("shows the saved catalog on the picker, even when it isn't listed", () => {
    const html = renderForm({
      catalogList: READY,
      initialValues: { catalog: "finance" },
    });

    expect(html).toContain(">finance<");
  });

  test("is disabled while catalogs are loading", () => {
    const html = renderForm({ catalogList: { status: "loading" } });

    expect(html).toMatch(/<button[^>]*role="combobox"[^>]*disabled=""/);
  });

  test("falls back to a text input when catalogs can't be listed", () => {
    const html = renderForm({ catalogList: { status: "unavailable" } });

    expect(html).toContain('id="setup-catalog"');
    expect(html).toContain('name="catalog"');
    expect(html).not.toContain('role="combobox"');
    expect(html).toContain(
      escapeHtml(en.setup.configuration.catalogListUnavailable),
    );
  });

  test("falls back to a text input when no catalog list is provided", () => {
    const html = renderForm();

    expect(html).toContain('name="catalog"');
    expect(html).not.toContain('role="combobox"');
  });
});

describe("group copy", () => {
  test("names the group field the default user group", () => {
    const html = renderForm();

    expect(en.setup.configuration.audience_group).toBe("Default user group");
    expect(html).toContain(en.setup.configuration.audience_group);
    expect(html).toContain(escapeHtml(en.setup.configuration.audience_groupHelp));
  });

  test("explains why a built-in group was rejected", () => {
    const html = renderForm({ errorCode: "audience_group_builtin" });

    expect(html).toContain(
      escapeHtml(en.setup.configuration.errors.audience_group_builtin),
    );
    expect(html).not.toContain(
      escapeHtml(en.setup.configuration.errors.default),
    );
  });

  test("no setup copy still calls the group an audience group", () => {
    expect(JSON.stringify(en.setup).toLowerCase()).not.toContain(
      "audience group",
    );
  });
});

describe("editing saved choices", () => {
  test("offers cancel only when editing saved choices", () => {
    expect(renderForm()).not.toContain(`>${en.common.cancel}<`);
    expect(renderForm({ onCancel: () => undefined })).toContain(
      `>${en.common.cancel}<`,
    );
  });
});
