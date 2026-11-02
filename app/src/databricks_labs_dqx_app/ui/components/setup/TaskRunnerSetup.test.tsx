import { describe, expect, test } from "bun:test";
import { MutationObserver, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import i18next from "i18next";
import { I18nextProvider } from "react-i18next";
import { renderToStaticMarkup } from "react-dom/server";

import { SetupWizard } from "./SetupWizard";
import {
  TaskRunnerSteps,
  accessResult,
  runAsCallbacks,
  type TaskRunnerStepsProps,
} from "./TaskRunnerSetup";
import {
  getGetSetupStatusQueryKey,
  getSetTaskRunnerRunAsMutationOptions,
  type SetupReport,
  type SetupStatusResponse,
} from "@/lib/api";
import en from "@/lib/i18n/locales/en.json";
import { setupView } from "@/lib/setup-state";

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

const RUNNER = "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5";
const HOST = "https://workspace.example.com";
const PICKED = { applicationId: RUNNER, displayName: "dqx-studio-task-runner", id: "42" };
const GRANT = `GRANT USE_CATALOG ON CATALOG \`main\` TO \`${RUNNER}\`;`;
const copy = en.setup.taskRunner;

function render(element: React.ReactNode): string {
  return renderToStaticMarkup(
    <I18nextProvider i18n={testI18n}>{element}</I18nextProvider>,
  );
}

function renderSteps(props: Partial<TaskRunnerStepsProps> = {}): string {
  return render(
    <TaskRunnerSteps
      principalList={{
        status: "ready",
        appName: "app-1a2b dqx-studio",
        principals: [
          { id: "42", display_name: "dqx-studio-task-runner", application_id: RUNNER },
        ],
      }}
      picked={null}
      onPick={() => undefined}
      workspaceHost={HOST}
      jobsLink={{ href: `${HOST}/jobs/7`, isTaskRunnerJob: true }}
      isBusy={false}
      access={null}
      isCheckingAccess={false}
      onConfirmAccess={() => undefined}
      isApplyingGrants={false}
      onApplyGrants={() => undefined}
      isApplying={false}
      onApply={() => undefined}
      {...props}
    />,
  );
}

function escapeHtml(text: string): string {
  return renderToStaticMarkup(<>{text}</>);
}

function button(markup: string, label: string): string {
  const end = markup.indexOf(`${escapeHtml(label)}</button>`);
  return markup.slice(markup.lastIndexOf("<button", end), end);
}

describe("TaskRunnerSteps", () => {
  test("shows the three steps, with how to create a service principal and where", () => {
    const html = renderSteps();

    expect(html).toContain(copy.steps.pick);
    expect(html).toContain(copy.steps.access);
    expect(html).toContain(copy.steps.apply);
    expect(html).toContain(escapeHtml(copy.pick.label));
    expect(html).toContain("<details");
    expect(html).toContain(escapeHtml(copy.pick.createTitle));
    expect(html).toContain(
      `href="${HOST}/settings/workspace/identity-and-access/service-principals"`,
    );
    expect(html).toContain(escapeHtml(copy.access.app.replace("{{appName}}", "app-1a2b dqx-studio")));
  });

  test("keeps steps 2 and 3 disabled until a service principal is picked", () => {
    const html = renderSteps();

    expect(button(html, copy.access.button)).toContain('disabled=""');
    expect(button(html, copy.apply.button)).toContain('disabled=""');
    expect(html).toContain(copy.pickFirst);
  });

  test("enables confirming access, but not applying, once a service principal is picked", () => {
    const html = renderSteps({ picked: PICKED });

    expect(button(html, copy.access.button)).not.toContain('disabled=""');
    expect(button(html, copy.apply.button)).toContain('disabled=""');
    expect(html).toContain(copy.apply.confirmFirst);
    expect(html).toContain(
      `href="${HOST}/settings/workspace/identity-and-access/service-principals/42"`,
    );
    expect(html).toContain(escapeHtml(copy.access.intro.replace("{{name}}", "dqx-studio-task-runner")));
  });

  test("confirms access and enables applying when nothing is missing", () => {
    const html = renderSteps({
      picked: PICKED,
      access: { sp_permission: "confirmed", grant_statements: [], automatic_grants: [], grants_unverified: false },
    });

    expect(html).toContain(copy.access.result.confirmed);
    expect(button(html, copy.apply.button)).not.toContain('disabled=""');
    expect(html).not.toContain(copy.apply.confirmFirst);
    expect(html).toContain(`href="${HOST}/jobs/7"`);
  });

  test("warns when the app seems to lack CAN USE, but still lets step 3 prove it", () => {
    const html = renderSteps({
      picked: PICKED,
      access: { sp_permission: "missing", grant_statements: [], automatic_grants: [], grants_unverified: false },
    });

    expect(html).toContain(escapeHtml(copy.access.result.missing));
    expect(button(html, copy.apply.button)).not.toContain('disabled=""');
    expect(html).not.toContain(copy.apply.confirmFirst);
  });

  test("lets an unverifiable permission continue to step 3", () => {
    const html = renderSteps({
      picked: PICKED,
      access: { sp_permission: "unverified", grant_statements: [], automatic_grants: [], grants_unverified: false },
    });

    expect(html).toContain(escapeHtml(copy.access.result.unverified));
    expect(button(html, copy.apply.button)).not.toContain('disabled=""');
  });

  test("shows pending grants as runnable SQL and flags them on step 3", () => {
    const html = renderSteps({
      picked: PICKED,
      access: { sp_permission: "confirmed", grant_statements: [GRANT], automatic_grants: [], grants_unverified: false },
    });

    expect(html).toContain(escapeHtml(copy.access.result.grantsPending));
    expect(html).toContain(escapeHtml(GRANT));
    expect(html).toContain(`aria-label="${en.sqlCodeBlock.run}"`);
    expect(html).toContain(escapeHtml(copy.apply.grantsStillMissing));
    expect(button(html, copy.apply.button)).not.toContain('disabled=""');
  });

  test("shows why running the grants failed", () => {
    const html = renderSteps({
      picked: PICKED,
      access: { sp_permission: "confirmed", grant_statements: [GRANT], automatic_grants: [], grants_unverified: false },
      grantOutcome: { summary: null, failedSql: GRANT, errorCodes: ["permission_denied"] },
    });

    expect(html).toContain(escapeHtml(en.setup.grants.errors.permission_denied));
  });

  test("tells the administrator to set Run as themselves when Studio isn't allowed", () => {
    const html = renderSteps({
      picked: PICKED,
      access: { sp_permission: "unverified", grant_statements: [], automatic_grants: [], grants_unverified: false },
      runAsErrorCode: "task_runner_run_as_forbidden",
    });

    expect(html).toContain(escapeHtml(copy.apply.errors.task_runner_run_as_forbidden));
  });

  test("falls back to pasting an Application ID when the list can't be loaded", () => {
    const html = renderSteps({ principalList: { status: "unavailable", appName: "dqx-studio" } });

    expect(html).toContain('name="application_id"');
    expect(html).not.toContain('role="combobox"');
    expect(html).toContain(escapeHtml(copy.pick.listUnavailable));
  });

  test("says when only the first service principals are listed, and that an ID can be pasted", () => {
    const html = renderSteps({
      principalList: {
        status: "ready",
        appName: "dqx-studio",
        principals: [{ id: "42", display_name: "dqx-studio-task-runner", application_id: RUNNER }],
        truncated: true,
      },
    });

    expect(html).toContain(escapeHtml(copy.pick.listTruncated_one.replace("{{count}}", "1")));
  });

  test("has no cut-list hint when every service principal is listed", () => {
    expect(renderSteps()).not.toContain(escapeHtml("If yours isn't listed, paste its Application ID."));
  });

  test("explains a refused access check without raw error text", () => {
    const html = renderSteps({ picked: PICKED, accessErrorCode: "task_runner_principal_is_app" });

    expect(html).toContain(escapeHtml(copy.access.errors.task_runner_principal_is_app));
  });
});

describe("accessResult", () => {
  test("fills in the optional fields", () => {
    expect(accessResult({ sp_permission: "confirmed" })).toEqual({
      sp_permission: "confirmed",
      grant_statements: [],
      automatic_grants: [],
      grants_unverified: false,
    });
  });
});

describe("automatic grants", () => {
  test("are explained, not offered to run", () => {
    const automatic = `GRANT SELECT ON SCHEMA \`main\`.\`dqx_studio_tmp\` TO \`${RUNNER}\`;`;
    const html = renderSteps({
      picked: PICKED,
      access: {
        sp_permission: "confirmed",
        grant_statements: [],
        automatic_grants: [automatic],
        grants_unverified: false,
      },
    });

    expect(html).toContain(escapeHtml(copy.access.result.automaticGrants));
    expect(html).not.toContain(escapeHtml(automatic));
    expect(html).not.toContain('aria-label="Run these commands with your Databricks access"');
  });
});

function statusWith(actions: string[]): SetupStatusResponse {
  return {
    can_manage: true,
    admin_group: "admins",
    report: {
      state: "setup_required",
      current_step: "task_runner",
      steps: [
        {
          id: "task_runner",
          state: "action_required",
          code: "task_runner_run_as_missing",
          summary: "Studio runs scheduled checks under a dedicated service principal.",
          actions: actions as SetupStatusResponse["report"]["steps"][number]["actions"],
        },
      ],
    },
  };
}

function renderWizard(status: SetupStatusResponse): string {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <SetupWizard
        view={setupView(status)}
        workspaceHost={HOST}
        isReconciling={false}
        onReconcile={() => undefined}
        reconciliationFailed={false}
      />
    </QueryClientProvider>,
  );
}

describe("task-runner step in the setup wizard", () => {
  test("offers the three steps for a job Studio created", () => {
    const html = renderWizard(statusWith(["choose_task_runner", "verify_again"]));

    expect(html).toContain(copy.steps.pick);
    expect(html).toContain(en.setup.actions.verify_again);
    expect(html).not.toContain("setup.actions.choose_task_runner");
  });

  test("offers no steps for a bundle-deployed job", () => {
    const html = renderWizard(statusWith(["verify_again"]));

    expect(html).not.toContain(copy.steps.pick);
    expect(html).toContain(en.setup.actions.verify_again);
  });
});

describe("runAsCallbacks", () => {
  test("posts the picked service principal and shows the re-checked report", async () => {
    const queryClient = new QueryClient();
    queryClient.setQueryData(getGetSetupStatusQueryKey(), { data: statusWith(["choose_task_runner"]) });
    const nextReport: SetupReport = {
      state: "setup_required",
      current_step: "task_runner",
      steps: [
        {
          id: "task_runner",
          state: "action_required",
          code: "task_runner_permissions_missing",
          grant_statements: [GRANT],
        },
      ],
    };
    const requests: { method?: string; url?: string; data?: unknown }[] = [];
    const mutation = new MutationObserver(
      queryClient,
      getSetTaskRunnerRunAsMutationOptions({
        mutation: runAsCallbacks(queryClient),
        axios: {
          adapter: async (config) => {
            requests.push({ method: config.method, url: config.url, data: config.data });
            return { data: nextReport, status: 200, statusText: "OK", headers: {}, config };
          },
        },
      }),
    );

    await mutation.mutate({ data: { application_id: RUNNER } });

    expect(requests).toEqual([
      {
        method: "post",
        url: "/api/v1/setup/task-runner/run-as",
        data: JSON.stringify({ application_id: RUNNER }),
      },
    ]);
    const cached = queryClient.getQueryData<{ data: SetupStatusResponse }>(getGetSetupStatusQueryKey());
    expect(cached?.data.report).toEqual(nextReport);
    expect(queryClient.getQueryState(getGetSetupStatusQueryKey())?.isInvalidated).toBe(true);
  });
});
