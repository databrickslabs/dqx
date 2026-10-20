import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { SetupConfigurationForm } from "./SetupConfigurationForm";
import en from "@/lib/i18n/locales/en.json";
import type { SetupOptionList } from "@/lib/setup-options";

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

const READY: SetupOptionList = { status: "ready", options: ["main", "sales"] };

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

  test("stays usable while catalogs are loading, so the name can be typed", () => {
    const html = renderForm({ catalogList: { status: "loading" } });

    expect(html).toMatch(/<button[^>]*id="setup-catalog"/);
    expect(html).toMatch(/<button[^>]*role="combobox"/);
    expect(html).not.toMatch(/<button[^>]*role="combobox"[^>]*disabled=""/);
    expect(html).toContain('aria-busy="true"');
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

describe("default user group field", () => {
  const GROUPS: SetupOptionList = { status: "ready", options: ["data-team", "ops"] };

  test("is a searchable picker labelled by the group label when groups are listed", () => {
    const html = renderForm({ groupList: GROUPS });

    expect(html).toMatch(/<button[^>]*id="setup-audience_group"[^>]*role="combobox"|<button[^>]*role="combobox"[^>]*id="setup-audience_group"/);
    expect(html).toContain('for="setup-audience_group"');
    expect(html).not.toContain('name="audience_group"');
    expect(html).toContain(en.setup.configuration.groupPlaceholder);
    expect(html).toContain(escapeHtml(en.setup.configuration.audience_groupHelp));
  });

  test("shows the saved group on the picker", () => {
    const html = renderForm({
      groupList: GROUPS,
      initialValues: { audience_group: "finance" },
    });

    expect(html).toContain(">finance<");
  });

  test("stays usable while groups are loading", () => {
    const html = renderForm({ groupList: { status: "loading" } });

    expect(html).toMatch(/<button[^>]*role="combobox"/);
    expect(html).not.toMatch(/<button[^>]*role="combobox"[^>]*disabled=""/);
  });

  test.each([
    ["unavailable", { status: "unavailable" } as SetupOptionList],
    ["not provided", undefined],
  ])("falls back to a text input when groups are %s", (_label, groupList) => {
    const html = renderForm({ groupList });

    expect(html).toContain('id="setup-audience_group"');
    expect(html).toContain('name="audience_group"');
    expect(html).not.toContain('id="setup-audience_group" role="combobox"');
    expect(html).not.toMatch(/<button[^>]*id="setup-audience_group"/);
  });

  test("says when only the first groups are listed, and that a name can be typed", () => {
    const html = renderForm({ groupList: { ...GROUPS, truncated: true } });

    expect(html).toContain(
      escapeHtml(en.setup.configuration.groupListTruncated_other.replace("{{count}}", "2")),
    );
  });

  test("has no cut-list hint when every group is listed", () => {
    const html = renderForm({ groupList: GROUPS });

    expect(html).not.toContain(escapeHtml("If yours isn't listed, type its name."));
  });

  test("leaves the catalog picker alone when only groups are listed", () => {
    const html = renderForm({ groupList: GROUPS });

    expect(html).toContain('name="catalog"');
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

  test.each([
    ["catalog_invalid", en.setup.configuration.errors.catalog_invalid],
    ["audience_group_required", en.setup.configuration.errors.audience_group_required],
    ["configuration_invalid", en.setup.configuration.errors.configuration_invalid],
  ])("shows its own message for %s", (code, message) => {
    const html = renderForm({ errorCode: code });

    expect(html).toContain(escapeHtml(message));
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
