import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import {
  SetupCompleteView,
  demoOffer,
  type SetupCompleteViewProps,
} from "./SetupComplete";
import en from "@/lib/i18n/locales/en.json";

Object.defineProperty(globalThis, "__APP_NAME__", { value: "DQX Studio" });
Object.defineProperty(globalThis, "localStorage", {
  configurable: true,
  value: { getItem: () => null, setItem: () => undefined },
});

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

const copy = en.setup.complete;

function render(props: Partial<SetupCompleteViewProps> = {}): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <SetupCompleteView
        offer="available"
        onDeploy={() => undefined}
        onContinue={() => undefined}
        {...props}
      />
    </I18nextProvider>,
  );
}

describe("demoOffer", () => {
  test("offers the demo unless it is deploying, running or already deployed", () => {
    expect(demoOffer("idle", false, false)).toBe("available");
    expect(demoOffer("failed", false, false)).toBe("available");
    expect(demoOffer("idle", true, false)).toBe("deploying");
    expect(demoOffer("running", false, false)).toBe("running");
    expect(demoOffer("succeeded", false, false)).toBe("deployed");
  });

  test("does not offer the demo until its status has loaded", () => {
    // A slow or failed status call must not start a second seed over an existing demo.
    expect(demoOffer(undefined, false, false)).toBe("unknown");
  });

  test("a deploy started from this screen wins over a stale status", () => {
    expect(demoOffer("idle", false, true)).toBe("started");
  });
});

describe("SetupCompleteView", () => {
  test("offers deploying the demo, or skipping straight to Studio", () => {
    const html = render();

    expect(html).toContain(copy.title);
    expect(html).toContain(copy.demo.deploy);
    expect(html).toContain(copy.skip);
    expect(html).not.toContain(copy.continue);
  });

  test("once the demo is started, only continuing remains", () => {
    const html = render({ offer: "started" });

    expect(html).toContain(copy.demo.started);
    expect(html).not.toContain(copy.demo.deploy);
    expect(html).toContain(copy.continue);
  });

  test("does not offer deploying while the demo's status is unknown", () => {
    const html = render({ offer: "unknown" });

    expect(html).not.toContain(copy.demo.deploy);
    expect(html).toContain(copy.continue);
    expect(html).not.toContain(copy.skip);
  });

  test("does not offer the demo again when it is already deployed", () => {
    const html = render({ offer: "deployed" });

    expect(html).toContain(copy.demo.deployed);
    expect(html).not.toContain(copy.demo.deploy);
  });

  test("shows why a deploy failed and lets the administrator retry", () => {
    const html = render({ errorMessage: copy.demo.failed });

    expect(html).toContain('role="alert"');
    expect(html).toContain(copy.demo.deploy);
  });
});
