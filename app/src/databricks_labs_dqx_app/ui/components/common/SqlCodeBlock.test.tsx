import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { SqlCodeBlock, copySqlToClipboard } from "./SqlCodeBlock";
import en from "@/lib/i18n/locales/en.json";

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

const SQL =
  "GRANT USE_CATALOG ON CATALOG `main` TO `data-team`;\nGRANT USE_SCHEMA ON SCHEMA `main`.`dqx_studio` TO `data-team`;";
const RUN_LABEL = `aria-label="${en.sqlCodeBlock.run}"`;

function render(element: React.ReactNode): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>{element}</I18nextProvider>,
  );
}

function runButton(markup: string): string {
  const start = markup.lastIndexOf("<button", markup.indexOf(RUN_LABEL));
  return markup.slice(start, markup.indexOf("</button>", start));
}

describe("SqlCodeBlock", () => {
  test("shows the SQL with a copy icon", () => {
    const markup = render(<SqlCodeBlock sql={SQL} />);

    expect(markup).toContain("<pre");
    expect(markup).toContain("GRANT USE_CATALOG ON CATALOG `main` TO `data-team`;");
    expect(markup).toContain(
      "GRANT USE_SCHEMA ON SCHEMA `main`.`dqx_studio` TO `data-team`;",
    );
    expect(markup).toContain('aria-label="Copy"');
  });

  test("has no run icon without a run handler", () => {
    expect(render(<SqlCodeBlock sql={SQL} />)).not.toContain(RUN_LABEL);
  });

  test("offers a run icon when it can run the SQL", () => {
    const button = runButton(
      render(<SqlCodeBlock sql={SQL} onRun={() => undefined} />),
    );

    expect(button).not.toContain('disabled=""');
    expect(button).not.toContain("animate-spin");
  });

  test("disables the run icon and shows a spinner while running", () => {
    const button = runButton(
      render(<SqlCodeBlock sql={SQL} onRun={() => undefined} isRunning />),
    );

    expect(button).toContain('disabled=""');
    expect(button).toContain("animate-spin");
  });

  test("disables the run icon without a spinner when asked to", () => {
    const button = runButton(
      render(
        <SqlCodeBlock sql={SQL} onRun={() => undefined} runDisabled />,
      ),
    );

    expect(button).toContain('disabled=""');
    expect(button).not.toContain("animate-spin");
  });

  test("renders the result directly under the code", () => {
    const markup = render(
      <SqlCodeBlock sql={SQL} result={<p>Done: 1 of 2.</p>} />,
    );

    expect(markup.indexOf("Done: 1 of 2.")).toBeGreaterThan(
      markup.indexOf("</pre>"),
    );
  });
});

describe("copySqlToClipboard", () => {
  test("writes the SQL to the given clipboard", async () => {
    const written: string[] = [];

    const copied = await copySqlToClipboard(SQL, {
      writeText: async (text) => {
        written.push(text);
      },
    });

    expect(copied).toBe(true);
    expect(written).toEqual([SQL]);
  });

  test("uses the browser clipboard by default", async () => {
    const written: string[] = [];
    const previous = Object.getOwnPropertyDescriptor(globalThis, "navigator");
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: {
        clipboard: {
          writeText: async (text: string) => {
            written.push(text);
          },
        },
      },
    });
    try {
      expect(await copySqlToClipboard(SQL)).toBe(true);
    } finally {
      if (previous) Object.defineProperty(globalThis, "navigator", previous);
    }

    expect(written).toEqual([SQL]);
  });

  test("reports a failed copy", async () => {
    const copied = await copySqlToClipboard(SQL, {
      writeText: () => Promise.reject(new Error("denied")),
    });

    expect(copied).toBe(false);
  });

  test("reports a missing clipboard", async () => {
    expect(await copySqlToClipboard(SQL, null)).toBe(false);
  });
});
