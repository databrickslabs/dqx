import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { TaskRunnerPrincipalPicker } from "./TaskRunnerPrincipalPicker";
import en from "@/lib/i18n/locales/en.json";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

const PRINCIPALS = [
  {
    id: "1",
    display_name: "dqx-studio-task-runner",
    application_id: "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5",
  },
];

function render(
  props: Partial<React.ComponentProps<typeof TaskRunnerPrincipalPicker>> = {},
): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <TaskRunnerPrincipalPicker
        id="task-runner-principal"
        value={null}
        principals={PRINCIPALS}
        isLoading={false}
        onChange={() => undefined}
        {...props}
      />
    </I18nextProvider>,
  );
}

describe("TaskRunnerPrincipalPicker", () => {
  test("is a combobox with a placeholder until a service principal is picked", () => {
    const html = render();

    expect(html).toContain('role="combobox"');
    expect(html).toContain('id="task-runner-principal"');
    expect(html).toContain(en.setup.taskRunner.pick.placeholder);
  });

  test("shows the picked service principal's name", () => {
    const html = render({
      value: {
        applicationId: PRINCIPALS[0].application_id,
        displayName: "dqx-studio-task-runner",
        id: "1",
      },
    });

    expect(html).toContain(">dqx-studio-task-runner<");
    expect(html).not.toContain(en.setup.taskRunner.pick.placeholder);
  });

  test("is disabled while loading or busy", () => {
    expect(render({ isLoading: true })).toMatch(/<button[^>]*role="combobox"[^>]*disabled=""/);
    expect(render({ disabled: true })).toMatch(/<button[^>]*role="combobox"[^>]*disabled=""/);
  });
});
