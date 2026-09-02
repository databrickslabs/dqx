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
  invalidateSetupStatus,
  reconciliationMutationOptions,
  setupPollingInBackground,
  setupPollingInterval,
} from "./SetupGate";
import { SetupShell, SetupWizard } from "./setup/SetupWizard";
import { getGetSetupStatusQueryKey, type SetupStatusResponse } from "@/lib/api";
import { getWorkspaceHostQueryKey } from "@/lib/api-custom";
import { useTheme } from "@/components/layout/theme-provider";
import en from "@/lib/i18n/locales/en.json";
import { setupView } from "@/lib/setup-state";

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
        id: "volume",
        state: "action_required",
        summary: "Grant access to the wheels volume.",
        actions: ["verify_again"],
      },
    ];

    const markup = renderGate(status);

    expect(markup).not.toContain("Why this is needed");
    expect(markup).toContain(
      'aria-label="Stores the DQX Core library and task-runner application used by profiling and data-quality jobs."',
    );
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

  test("polls only while setup is checking or initializing", () => {
    expect(setupPollingInterval("checking")).toBe(2_000);
    expect(setupPollingInterval("initializing")).toBe(2_000);
    expect(setupPollingInterval("setup_required")).toBe(false);
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
