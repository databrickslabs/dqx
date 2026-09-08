import { describe, expect, test } from "bun:test";

import en from "./i18n/locales/en.json";
import es from "./i18n/locales/es.json";
import fr from "./i18n/locales/fr.json";
import it from "./i18n/locales/it.json";
import ptBR from "./i18n/locales/pt-BR.json";
import { setupView } from "./setup-state";
import type { SetupActionId, SetupReport, SetupState, SetupStatusResponse } from "./api";

function report(state: SetupState): SetupReport {
  return {
    state,
    steps: [
      {
        id: "identity",
        state: "action_required",
        summary: "The app service principal identity needs attention.",
      },
    ],
    current_step: "identity",
  };
}

function reportWithAction(action: SetupActionId): SetupReport {
  return {
    ...report("setup_required"),
    steps: [
      {
        id: "identity",
        state: "action_required",
        actions: [action],
      },
    ],
  };
}

function status(reportValue: SetupReport, canManage: boolean): SetupStatusResponse {
  return {
    report: reportValue,
    can_manage: canManage,
    admin_group: "dqx-admins",
  };
}

describe("setupView", () => {
  test("ready passes through without wizard", () => {
    expect(setupView(status(report("ready"), true)).kind).toBe("ready");
  });

  test("non-admin sees waiting state without actions", () => {
    const view = setupView(status(report("setup_required"), false));

    expect(view.kind).toBe("waiting");
    expect(view.actions).toEqual([]);
  });

  test("admin receives only backend-advertised actions", () => {
    const view = setupView(status(reportWithAction("verify_again"), true));

    expect(view.actions.map((action) => action.id)).toEqual(["verify_again"]);
  });
});

function leafKeys(value: unknown, prefix = ""): string[] {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return [prefix];

  return Object.entries(value)
    .flatMap(([key, nested]) => leafKeys(nested, prefix ? `${prefix}.${key}` : key))
    .sort();
}

describe("locale keys", () => {
  test("match English recursively in every supported locale", () => {
    const englishKeys = leafKeys(en);

    expect(englishKeys).toContain("setup.title");
    expect(leafKeys(fr)).toEqual(englishKeys);
    expect(leafKeys(ptBR)).toEqual(englishKeys);
    expect(leafKeys(it)).toEqual(englishKeys);
    expect(leafKeys(es)).toEqual(englishKeys);
  });
});
