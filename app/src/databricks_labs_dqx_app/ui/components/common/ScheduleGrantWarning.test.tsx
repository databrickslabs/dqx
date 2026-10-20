import { describe, expect, test } from "bun:test";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { ScheduleGrantWarning } from "./ScheduleGrantWarning";
import type { ScheduleGrantPreflight } from "@/components/schedules/useScheduleGrantPreflight";
import type { SchedulePreflightTableOut } from "@/lib/api";
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

const STATEMENT = "GRANT USE_SCHEMA ON SCHEMA `c`.`s` TO `runner-sp`;";

const TABLE: SchedulePreflightTableOut = {
  fqn: "c.s.t",
  can_manage: false,
  missing_grants: [
    {
      principal: "runner-sp",
      display_name: "dqx-task-runner-sp",
      privilege: "USE_SCHEMA",
      securable_type: "SCHEMA",
      securable: "c.s",
      statement: STATEMENT,
    },
  ],
};

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#x27;");
}

const RUN_LABEL = `aria-label="${escapeHtml(en.schedule.grantWarning.apply.run)}"`;

function preflight(
  overrides: Partial<ScheduleGrantPreflight> = {},
): ScheduleGrantPreflight {
  return {
    blockedTables: [],
    missingGrantTables: [TABLE],
    unverifiedTables: [],
    hasGrantIssue: true,
    isFetching: false,
    retry: () => undefined,
    applyGrants: () => undefined,
    isApplying: false,
    applyOutcome: null,
    recheckFailed: null,
    ...overrides,
  };
}

function render(value: ScheduleGrantPreflight): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>
      <ScheduleGrantWarning entity="table" preflight={value} />
    </I18nextProvider>,
  );
}

function runButton(markup: string): string {
  const start = markup.lastIndexOf("<button", markup.indexOf(RUN_LABEL));
  return markup.slice(start, markup.indexOf("</button>", start));
}

function retryButton(markup: string): string {
  const label = markup.indexOf(`>${en.schedule.grantWarning.retry}</button>`);
  const start = markup.lastIndexOf("<button", label);
  return markup.slice(start, label);
}

describe("ScheduleGrantWarning missing grants", () => {
  test("shows the statements with a run icon that explains what it does", () => {
    const markup = render(preflight());

    expect(markup).toContain(escapeHtml(STATEMENT));
    expect(markup).toContain(RUN_LABEL);
    expect(runButton(markup)).not.toContain('disabled=""');
  });

  test("shows a busy run icon while the grants run", () => {
    const button = runButton(render(preflight({ isApplying: true })));

    expect(button).toContain('disabled=""');
    expect(button).toContain("animate-spin");
  });

  test("Try again is disabled while the grants run", () => {
    expect(retryButton(render(preflight()))).not.toContain('disabled=""');
    expect(retryButton(render(preflight({ isApplying: true })))).toContain('disabled=""');
    expect(retryButton(render(preflight({ isFetching: true })))).toContain('disabled=""');
  });

  test("a failed re-check still says how many grants ran", () => {
    const markup = render(preflight({ recheckFailed: { applied: 2, total: 3 } }));

    expect(markup).toContain(
      escapeHtml("Done: 2 of 3. Studio couldn't check the result just now. Click Try again to refresh."),
    );
  });

  test.each([
    ["fr", fr],
    ["es", es],
    ["it", it],
    ["pt-BR", ptBR],
  ] as const)("the failed re-check keeps its counts in %s", (_code, locale) => {
    const message = locale.schedule.grantWarning.apply.recheckFailed;

    expect(message).toContain("{{applied}}");
    expect(message).toContain("{{total}}");
    expect(message).not.toBe(en.schedule.grantWarning.apply.recheckFailed);
  });

  test("a permission failure explains who can help and keeps the SQL visible", () => {
    const markup = render(
      preflight({
        applyOutcome: {
          summary: { applied: 1, total: 2 },
          failedSql: STATEMENT,
          errorCodes: ["permission_denied"],
        },
      }),
    );

    expect(markup).toContain(
      escapeHtml(en.schedule.grantWarning.apply.errors.permission_denied),
    );
    expect(markup).toContain("Done: 1 of 2.");
    // The commands stay visible once, in the code cell the message points at ("the above commands").
    expect(markup.split(escapeHtml(STATEMENT)).length - 1).toBe(1);
    const message = markup.indexOf(
      escapeHtml(en.schedule.grantWarning.apply.errors.permission_denied),
    );
    expect(markup.indexOf(escapeHtml(STATEMENT))).toBeLessThan(message);
  });

  test.each(["warehouse_unavailable", "not_found", "rate_limited", "unknown"] as const)(
    "has its own message for %s",
    (code) => {
      const markup = render(
        preflight({
          applyOutcome: { summary: null, failedSql: "", errorCodes: [code] },
        }),
      );

      expect(markup).toContain(
        escapeHtml(en.schedule.grantWarning.apply.errors[code]),
      );
    },
  );
});
