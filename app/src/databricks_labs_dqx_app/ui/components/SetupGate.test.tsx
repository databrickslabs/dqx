import { describe, expect, test } from "bun:test";
import {
  MutationObserver,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import {
  SetupGate,
  applyGrantsCallbacks,
  invalidateSetupStatus,
  reconciliationMutationOptions,
  setupPollingInBackground,
  setupPollingInterval,
} from "./SetupGate";
import { SetupShell, SetupWizard, workspaceJobsLink } from "./setup/SetupWizard";
import {
  getApplySetupStepGrantsMutationOptions,
  getGetSetupStatusQueryKey,
  type SetupStatusResponse,
} from "@/lib/api";
import { getWorkspaceHostQueryKey } from "@/lib/api-custom";
import { useTheme } from "@/components/layout/theme-provider";
import en from "@/lib/i18n/locales/en.json";
import { setupView } from "@/lib/setup-state";
import type { SetupOptionList } from "@/lib/setup-options";

Object.defineProperty(globalThis, "__APP_NAME__", { value: "DQX Studio" });
Object.defineProperty(globalThis, "localStorage", {
  configurable: true,
  value: {
    getItem: () => null,
    setItem: () => undefined,
  },
});

const testI18n = i18next.createInstance();
void testI18n.init({
  lng: "en",
  resources: { en: { translation: en } },
  interpolation: { escapeValue: false },
  initImmediate: false,
});

function renderSetup(children: React.ReactNode): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>{children}</I18nextProvider>,
  );
}

function setupStatus(
  canManage: boolean,
  actions: string[] = [],
): SetupStatusResponse {
  return {
    can_manage: canManage,
    admin_group: "dqx-admins",
    report: {
      state: "setup_required",
      current_step: "task_runner",
      steps: [
        {
          id: "task_runner",
          state: "action_required",
          summary: "Assign the task-runner identity.",
          actions:
            actions as SetupStatusResponse["report"]["steps"][number]["actions"],
        },
      ],
    },
  };
}

function renderGate(
  status: SetupStatusResponse,
  workspaceHost?: string,
): string {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  queryClient.setQueryData(getGetSetupStatusQueryKey(), { data: status });
  if (workspaceHost) {
    queryClient.setQueryData(getWorkspaceHostQueryKey(), {
      data: { workspace_host: workspaceHost },
    });
  }

  return renderSetup(
    <QueryClientProvider client={queryClient}>
      <SetupGate>
        <p>Studio content</p>
      </SetupGate>
    </QueryClientProvider>,
  );
}

describe("SetupGate", () => {
  test("renders children only after setup is ready", () => {
    const status = setupStatus(true);
    status.report.state = "ready";

    expect(renderGate(status)).toContain("Studio content");
  });

  test("keeps reconciliation controls out of the non-admin setup view", () => {
    const markup = renderGate(setupStatus(false, ["verify_again"]));

    expect(markup).not.toContain("setup.actions.verify_again");
    expect(markup).not.toContain("setup.actions.openJobs");
  });

  test("explains remediation instead of checking in the administrator wizard", () => {
    const markup = renderGate(setupStatus(true));

    expect(markup).toContain(
      "Complete the required steps below to make DQX Studio available.",
    );
    expect(markup).not.toContain(
      "DQX Studio is checking the capabilities required to start safely.",
    );
  });

  test("renders backend actions disabled while reconciliation is in flight", () => {
    const view = setupView(setupStatus(true, ["verify_again"]));
    const markup = renderSetup(
      <SetupWizard
        view={view}
        isReconciling
        onReconcile={() => undefined}
        reconciliationFailed={false}
      />,
    );

    expect(markup).toContain('disabled=""');
  });

  test("keeps each setup resource explanation in an accessible tooltip", () => {
    const status = setupStatus(true);
    status.report.steps = [
      {
        id: "storage",
        state: "action_required",
        summary: "Grant access to the storage schemas.",
        actions: ["verify_again"],
      },
    ];

    const markup = renderGate(status);

    expect(markup).not.toContain("Why this is needed");
    expect(markup).toContain(`aria-label="${en.setup.purposes.storage}"`);
  });

  test("renders the active setup step while initialization is running", () => {
    const status = setupStatus(true);
    status.report.state = "initializing";
    status.report.current_step = "wheels";
    status.report.steps = [
      {
        id: "task_runner",
        state: "passed",
        summary: "The task-runner job is ready.",
      },
    ];

    const markup = renderGate(status);

    expect(markup).toContain("Task-runner job");
    expect(markup).toContain("Application wheels");
    expect(markup).toContain("In progress");
  });

  test("builds the Jobs link from the absolute workspace host", () => {
    const markup = renderGate(
      setupStatus(true),
      "https://workspace.example.com",
    );

    expect(markup).toContain('href="https://workspace.example.com/#job/list"');
  });

  test("links straight to the task-runner job once setup knows it", () => {
    const status = setupStatus(true);
    status.task_runner_job_id = 914148714803833;

    const markup = renderGate(status, "https://workspace.example.com/");

    expect(markup).toContain(
      'href="https://workspace.example.com/jobs/914148714803833"',
    );
    expect(markup).toContain(en.setup.actions.openTaskRunnerJob);
  });

  test("tells a non-administrator who they are and who can finish setup", () => {
    const status = setupStatus(false);
    status.user_name = "analyst@example.com";
    status.report.steps.push({
      id: "configuration",
      state: "action_required",
      code: "configuration_required",
      summary: "Choose the catalog, storage prefix and default user group for DQX Studio.",
      actions: ["configure"],
    });

    const markup = renderGate(status);

    expect(markup).toContain(en.setup.notAdmin.title.replace("'", "&#x27;"));
    expect(markup).toContain("analyst@example.com");
    expect(markup).toContain("dqx-admins");
    expect(markup).toContain(en.setup.notAdmin.configurationPending);
    expect(markup).not.toContain("Choose the catalog, storage prefix");
    expect(markup).not.toContain('name="catalog"');
  });

  test("non-administrators get the waiting message for any configuration problem, with its code", () => {
    const status = setupStatus(false);
    status.report.steps.push({
      id: "configuration",
      state: "action_required",
      code: "saved_configuration_invalid",
      summary: "The Studio catalog, storage prefix or default user group isn't valid.",
    });

    const markup = renderGate(status);

    expect(markup).toContain(en.setup.notAdmin.configurationPending);
    expect(markup).toContain("saved_configuration_invalid");
    expect(markup).not.toContain("storage prefix or default user group isn");
  });

  test.each(["failed", "warning"] as const)(
    "non-administrators get the waiting message when configuration is %p",
    (state) => {
      const status = setupStatus(false);
      status.report.steps.push({
        id: "configuration",
        state,
        code: "configuration_unreadable",
        summary: "Fix the catalog, storage prefix or default user group, then click Verify again.",
      });

      const markup = renderGate(status);

      expect(markup).toContain(en.setup.notAdmin.configurationPending);
      expect(markup).toContain("configuration_unreadable");
      expect(markup).not.toContain("Fix the catalog");
    },
  );

  test("names a custom administrator group once", () => {
    const markup = renderGate(setupStatus(false));

    expect(markup.split("Only workspace admins").length - 1).toBe(1);
    expect(markup).toContain("members of dqx-admins");
  });

  test.each(["", "admins", "Admins"])(
    "uses the admins-only wording when the group is %p",
    (group) => {
      const status = setupStatus(false);
      status.admin_group = group;
      status.user_name = "analyst@example.com";

      const markup = renderGate(status);

      expect(markup).toContain("Only workspace admins can set up DQX Studio");
      expect(markup).toContain("analyst@example.com");
      expect(markup).not.toContain("members of");
    },
  );

  test("labels the link by what it opens", () => {
    expect(workspaceJobsLink("https://host/", 42)).toEqual({
      href: "https://host/jobs/42",
      isTaskRunnerJob: true,
    });
    expect(workspaceJobsLink("https://host", 0)).toEqual({
      href: "https://host/#job/list",
      isTaskRunnerJob: false,
    });
    expect(workspaceJobsLink("", 42)).toBeNull();
  });

  test("administrators never see the not-admin notice", () => {
    const markup = renderGate(setupStatus(true));

    expect(markup).not.toContain(en.setup.notAdmin.title.replace("'", "&#x27;"));
  });

  test("polls slowly while setup is waiting for external action", () => {
    expect(setupPollingInterval("checking")).toBe(2_000);
    expect(setupPollingInterval("initializing")).toBe(2_000);
    expect(setupPollingInterval("setup_required")).toBe(10_000);
    expect(setupPollingInterval("ready")).toBe(false);
  });

  test("continues polling while reconciliation is in flight", () => {
    expect(setupPollingInterval("setup_required", true)).toBe(2_000);
  });

  test("continues active setup polling while the wizard tab is backgrounded", () => {
    expect(setupPollingInBackground()).toBe(true);
  });

  test("invalidates setup status after reconciliation settles", async () => {
    const queryClient = new QueryClient();
    queryClient.setQueryData(getGetSetupStatusQueryKey(), {
      data: setupStatus(true),
    });

    await invalidateSetupStatus(queryClient);

    expect(
      queryClient.getQueryState(getGetSetupStatusQueryKey())?.isInvalidated,
    ).toBe(true);
  });

  test("invalidates setup status when reconciliation resolves", async () => {
    const queryClient = new QueryClient();
    queryClient.setQueryData(getGetSetupStatusQueryKey(), {
      data: setupStatus(true),
    });
    const mutation = new MutationObserver(
      queryClient,
      reconciliationMutationOptions(queryClient, () =>
        Promise.resolve(undefined),
      ),
    );

    await mutation.mutate();

    expect(
      queryClient.getQueryState(getGetSetupStatusQueryKey())?.isInvalidated,
    ).toBe(true);
  });

  test("invalidates setup status when reconciliation rejects", async () => {
    const queryClient = new QueryClient();
    queryClient.setQueryData(getGetSetupStatusQueryKey(), {
      data: setupStatus(true),
    });
    const mutation = new MutationObserver(
      queryClient,
      reconciliationMutationOptions(queryClient, () =>
        Promise.reject(new Error("reconciliation failed")),
      ),
    );

    await expect(mutation.mutate()).rejects.toThrow("reconciliation failed");

    expect(
      queryClient.getQueryState(getGetSetupStatusQueryKey())?.isInvalidated,
    ).toBe(true);
  });

  test("provides the configured theme context to the setup shell", () => {
    const writes: [string, string][] = [];
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: {
        getItem: () => null,
        setItem: (key: string, value: string) => writes.push([key, value]),
      },
    });

    function ThemeProbe() {
      const { setTheme } = useTheme();
      setTheme("light");
      return null;
    }

    renderSetup(
      <SetupShell>
        <ThemeProbe />
      </SetupShell>,
    );

    expect(writes).toEqual([["cdh-ui-theme", "light"]]);
  });
});

function configurationStatus(canManage: boolean): SetupStatusResponse {
  return {
    can_manage: canManage,
    admin_group: "admins",
    configuration: {
      source: "none",
      catalog: "",
      prefix: "",
      audience_group: "",
      schemas: [],
      broad_audience: false,
      locked: false,
    },
    report: {
      state: "setup_required",
      current_step: "configuration",
      steps: [
        { id: "identity", state: "passed" },
        { id: "lakebase", state: "passed" },
        {
          id: "configuration",
          state: "action_required",
          code: "configuration_required",
          actions: ["configure"],
        },
      ],
    },
  };
}

function renderWizard(
  status: SetupStatusResponse,
  {
    editingConfiguration = false,
    catalogList,
  }: { editingConfiguration?: boolean; catalogList?: SetupOptionList } = {},
): string {
  return renderSetup(
    <SetupWizard
      view={setupView(status, new Set(), editingConfiguration)}
      isReconciling={false}
      onReconcile={() => undefined}
      reconciliationFailed={false}
      catalogList={catalogList}
      onEditConfiguration={() => undefined}
    />,
  );
}

describe("setup configuration", () => {
  test("admins see the configuration form", () => {
    const html = renderWizard(configurationStatus(true));
    expect(html).toContain('name="catalog"');
    expect(html).toContain('value="dqx_studio"');
    expect(html).toContain('name="audience_group"');
  });

  test("the form picks the catalog from the listed catalogs", () => {
    const html = renderWizard(configurationStatus(true), {
      catalogList: { status: "ready", options: ["main"] },
    });
    expect(html).toContain('role="combobox"');
    expect(html).not.toContain('name="catalog"');
  });

  test("non-admins never see the form", () => {
    const html = renderWizard(configurationStatus(false));
    expect(html).not.toContain('name="catalog"');
  });

  test("deployment configuration is read-only", () => {
    const status = configurationStatus(true);
    status.configuration = {
      ...status.configuration!,
      source: "deployment",
      catalog: "main",
      prefix: "dqx_studio",
      broad_audience: true,
    };
    status.report.steps[2] = { id: "configuration", state: "passed" };
    const html = renderWizard(status);
    expect(html).not.toContain('name="catalog"');
    expect(html).toContain(en.setup.configuration.broadAudience);
  });

  test("warning steps are labelled", () => {
    const status = configurationStatus(true);
    status.report.steps.push({
      id: "app_sharing",
      state: "warning",
      code: "app_sharing_unverified",
      instructions: ["Share the app"],
    });
    const html = renderWizard(status);
    expect(html).toContain(en.setup.states.warning);
  });
});

function escapeHtml(text: string): string {
  return renderToStaticMarkup(<>{text}</>);
}

function readyWithWarning(canManage: boolean): SetupStatusResponse {
  return {
    can_manage: canManage,
    admin_group: "dqx-admins",
    report: {
      state: "ready",
      steps: [
        { id: "access", state: "passed" },
        {
          id: "app_sharing",
          state: "warning",
          code: "app_sharing_unverified",
          summary: "Could not verify that Studio users can open the app.",
          instructions: ["Share the app dqx-studio with group data-team."],
          actions: ["override"],
        },
      ],
    },
  };
}

describe("setup warnings review", () => {
  test("administrators review unacknowledged warnings before entering Studio", () => {
    const markup = renderGate(readyWithWarning(true));

    expect(markup).not.toContain("Studio content");
    expect(markup).toContain(en.setup.warningsReview.title);
    expect(markup).toContain(en.setup.steps.app_sharing);
    expect(markup).toContain(
      "Could not verify that Studio users can open the app.",
    );
    expect(markup).toContain("Share the app dqx-studio with group data-team.");
    expect(markup).toContain(escapeHtml(en.setup.override.button.app_sharing));
    expect(markup).not.toContain(`>${en.setup.actions.verify_again}</button>`);
    expect(markup).toContain(en.setup.warningsReview.acknowledge);
  });

  test("never shows setup warnings to non-administrators", () => {
    const markup = renderGate(readyWithWarning(false));

    expect(markup).toContain("Studio content");
    expect(markup).not.toContain(en.setup.warningsReview.title);
    expect(markup).not.toContain("Share the app");
  });

  test("enters Studio directly when the ready report has no warnings", () => {
    const status = readyWithWarning(true);
    status.report.steps = [{ id: "app_sharing", state: "passed" }];

    const markup = renderGate(status);

    expect(markup).toContain("Studio content");
    expect(markup).not.toContain(en.setup.warningsReview.title);
  });

  test("warning state has its own label", () => {
    expect(en.setup.states.warning).not.toBe(en.setup.states.failed);
    expect(en.setup.states.warning).toBe("Warning");
  });
});

describe("editable saved configuration", () => {
  function savedStatus(): SetupStatusResponse {
    const status = configurationStatus(true);
    status.configuration = {
      source: "saved",
      catalog: "main",
      prefix: "custom_prefix",
      audience_group: "data-team",
      schemas: ["custom_prefix", "custom_prefix_tmp"],
      broad_audience: false,
      locked: false,
    };
    status.report.current_step = "storage";
    status.report.steps = [
      { id: "identity", state: "passed" },
      { id: "configuration", state: "passed", actions: ["configure"] },
      {
        id: "storage",
        state: "action_required",
        code: "storage_collision",
        actions: ["verify_again"],
      },
    ];
    return status;
  }

  test("after saving, the passed step shows the saved choices and a change button, not the form", () => {
    const html = renderWizard(savedStatus());

    expect(html).not.toContain('name="catalog"');
    expect(html).not.toContain(`>${en.setup.configuration.submit}<`);
    expect(html).toContain("custom_prefix_tmp");
    expect(html).toContain(`>${en.setup.configuration.edit}</button>`);
  });

  test("choosing to change saved choices opens a prefilled form that can be cancelled", () => {
    const html = renderWizard(savedStatus(), { editingConfiguration: true });

    expect(html).toContain('name="catalog"');
    expect(html).toContain('value="main"');
    expect(html).toContain('value="custom_prefix"');
    expect(html).toContain('value="data-team"');
    expect(html).toContain(`>${en.common.cancel}</button>`);
    expect(html).not.toContain(`>${en.setup.configuration.edit}</button>`);
  });

  test("a saved configuration without configure stays read-only", () => {
    const status = savedStatus();
    status.report.steps[1] = { id: "configuration", state: "passed" };

    const html = renderWizard(status);
    expect(html).not.toContain('name="catalog"');
    expect(html).not.toContain(`>${en.setup.configuration.edit}</button>`);
  });
});

describe("setup overrides", () => {
  function blockedStatus(
    stepId: SetupStatusResponse["report"]["steps"][number]["id"],
  ): SetupStatusResponse {
    return {
      can_manage: true,
      admin_group: "dqx-admins",
      report: {
        state: "setup_required",
        current_step: stepId,
        steps: [
          {
            id: stepId,
            state: "action_required",
            code: "catalog_permissions_missing",
            summary: "Studio users don't have the permissions they need.",
            actions: ["verify_again", "override"],
          },
        ],
      },
    };
  }

  function renderWithOverride(status: SetupStatusResponse): string {
    return renderSetup(
      <SetupWizard
        view={setupView(status)}
        isReconciling={false}
        onReconcile={() => undefined}
        onOverride={() => undefined}
        reconciliationFailed={false}
      />,
    );
  }

  test("offers continue anyway with an explanation of inherited grants", () => {
    const markup = renderWithOverride(blockedStatus("unity_catalog"));

    expect(markup).toContain(en.setup.actions.verify_again);
    expect(markup).toContain(en.setup.override.button.default);
    expect(markup).toContain(escapeHtml(en.setup.override.help.default));
  });

  test("uses AI-specific wording for the AI step", () => {
    const markup = renderWithOverride(blockedStatus("ai"));

    expect(markup).toContain(en.setup.override.button.ai);
    expect(markup).toContain(en.setup.steps.ai);
  });

  test("hides the override without a handler", () => {
    const markup = renderSetup(
      <SetupWizard
        view={setupView(blockedStatus("warehouse"))}
        isReconciling={false}
        onReconcile={() => undefined}
        reconciliationFailed={false}
      />,
    );

    expect(markup).toContain(en.setup.actions.verify_again);
    expect(markup).not.toContain(en.setup.override.button.default);
  });

  test("never offers overrides to non-administrators", () => {
    const status = blockedStatus("access");
    status.can_manage = false;

    expect(renderGate(status)).not.toContain(en.setup.override.button.default);
  });

  test("labels overridden steps as confirmed by an administrator", () => {
    const status = blockedStatus("warehouse");
    status.report.state = "ready";
    status.report.steps = [
      {
        id: "warehouse",
        state: "overridden",
        summary: "An administrator confirmed this is set up.",
        actions: ["verify_again"],
      },
    ];
    const markup = renderWithOverride(status);

    expect(markup).toContain(en.setup.states.overridden);
  });
});

describe("setup grant statements", () => {
  const GRANTS = [
    "GRANT USE_CATALOG ON CATALOG `main` TO `runner`;",
    "GRANT USE_SCHEMA ON SCHEMA `main`.`dqx_studio` TO `runner`;",
    "GRANT READ_VOLUME ON VOLUME `main`.`dqx_studio`.`wheels` TO `runner`;",
  ];
  const RUN_LABEL = `aria-label="${en.sqlCodeBlock.run}"`;

  function grantStatus(canManage = true): SetupStatusResponse {
    return {
      can_manage: canManage,
      admin_group: "dqx-admins",
      report: {
        state: "setup_required",
        current_step: "task_runner",
        steps: [
          {
            id: "task_runner",
            state: "action_required",
            code: "task_runner_permissions_missing",
            summary: "The task runner doesn't have all the permissions it needs.",
            instructions: ["Studio needs the access below before it can continue."],
            grant_statements: GRANTS,
            actions: ["verify_again"],
          },
        ],
      },
    };
  }

  function renderGrants(
    props: Partial<React.ComponentProps<typeof SetupWizard>> = {},
    status: SetupStatusResponse = grantStatus(),
  ): string {
    return renderSetup(
      <SetupWizard
        view={setupView(status)}
        isReconciling={false}
        onReconcile={() => undefined}
        reconciliationFailed={false}
        onApplyGrants={() => undefined}
        {...props}
      />,
    );
  }

  test("shows the prose and every statement in one copyable code block", () => {
    const markup = renderGrants();

    expect(markup).toContain("Studio needs the access below before it can continue.");
    expect(markup).toContain(GRANTS.join("\n"));
    // The prose is plain text; only the statements are a code block.
    expect(markup.split("<pre").length - 1).toBe(1);
    expect(markup).toContain(`aria-label="${en.common.copy}"`);
    expect(markup).toContain(RUN_LABEL);
  });

  test("only administrators can run the statements", () => {
    expect(renderGate(grantStatus(false))).not.toContain(RUN_LABEL);
    expect(renderGate(grantStatus(false))).toContain(GRANTS[0]);
    expect(renderGrants({ onApplyGrants: undefined })).not.toContain(RUN_LABEL);
  });

  test("the line above the statements points to the Run button only when it's there", () => {
    const escape = (text: string) => text.replace(/'/g, "&#x27;");
    expect(renderGrants()).toContain(escape(en.setup.grants.leadRun));
    expect(renderGrants({ onApplyGrants: undefined })).toContain(escape(en.setup.grants.leadCopy));
    expect(renderGrants({ onApplyGrants: undefined })).not.toContain(escape(en.setup.grants.leadRun));
  });

  test("shows a spinner on the step whose statements are running", () => {
    const markup = renderGrants({ applyingGrantsStepId: "task_runner" });

    expect(markup).toContain("animate-spin");
    expect(markup).toContain('disabled=""');
  });

  test("explains a partial failure without repeating the step's commands", () => {
    const markup = renderGrants({
      grantOutcome: {
        stepId: "task_runner",
        outcome: {
          summary: { applied: 2, total: 3 },
          failedSql: GRANTS[1],
          errorCodes: ["permission_denied"],
        },
      },
    });

    expect(markup).toContain("Done: 2 of 3.");
    expect(markup).toContain(escapeHtml(en.setup.grants.errors.permission_denied));
    expect(markup).toContain('role="alert"');
    // The step's own code cell already shows the commands; the notice doesn't repeat them.
    expect(markup.split(GRANTS[1]).length - 1).toBe(1);
  });

  test("keeps another step's outcome off this step", () => {
    const markup = renderGrants({
      grantOutcome: {
        stepId: "access",
        outcome: { summary: null, failedSql: "", errorCodes: ["unknown"] },
      },
    });

    expect(markup).not.toContain(escapeHtml(en.setup.grants.errors.unknown));
  });

  test("the run icon calls the step's apply-grants endpoint and updates the report", async () => {
    const queryClient = new QueryClient();
    queryClient.setQueryData(getGetSetupStatusQueryKey(), { data: grantStatus() });
    const requests: { method?: string; url?: string; data?: unknown }[] = [];
    const nextReport = { state: "ready" as const, steps: [] };
    const mutation = new MutationObserver(
      queryClient,
      getApplySetupStepGrantsMutationOptions({
        mutation: applyGrantsCallbacks(queryClient),
        axios: {
          adapter: async (config) => {
            requests.push({ method: config.method, url: config.url, data: config.data });
            return {
              data: { report: nextReport, applied: 3, total: 3, failed: [] },
              status: 200,
              statusText: "OK",
              headers: {},
              config,
            };
          },
        },
      }),
    );

    await mutation.mutate({ stepId: "task_runner" });

    expect(requests).toEqual([
      { method: "post", url: "/api/v1/setup/steps/task_runner/apply-grants", data: undefined },
    ]);
    const cached = queryClient.getQueryData<{ data: SetupStatusResponse }>(
      getGetSetupStatusQueryKey(),
    );
    expect(cached?.data.report).toEqual(nextReport);
    expect(cached?.data.can_manage).toBe(true);
    expect(
      queryClient.getQueryState(getGetSetupStatusQueryKey())?.isInvalidated,
    ).toBe(true);
  });
});
