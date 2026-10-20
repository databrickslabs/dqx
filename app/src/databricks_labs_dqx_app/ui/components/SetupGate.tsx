import {
  useMutation,
  useQuery,
  useQueryClient,
  type QueryClient,
  type UseMutationOptions,
} from "@tanstack/react-query";

import { useState } from "react";
import type { AxiosError, AxiosResponse } from "axios";
import {
  configureSetup,
  getGetSetupStatusQueryKey,
  getSetupStatus,
  overrideSetupStep,
  reconcileSetup,
  useApplySetupStepGrants,
  useListCatalogs,
  useListSetupGroups,
  type HTTPValidationError,
  type SetupApplyGrantsResponse,
  type SetupStatusResponse,
  type SetupStepId,
} from "@/lib/api";
import {
  configurationListsWanted,
  setupErrorCode,
  setupView,
  unacknowledgedWarnings,
  warningKey,
} from "@/lib/setup-state";
import { setupOptionList } from "@/lib/setup-options";
import {
  grantRequestFailure,
  grantRunOutcome,
  type StepGrantOutcome,
} from "@/lib/setup-grants";
import {
  acknowledgeWarnings,
  readAcknowledgedWarnings,
} from "@/lib/setup-acknowledgements";
import { useWorkspaceHost } from "@/lib/api-custom";
import {
  SetupStatusUnavailable,
  SetupWizard,
} from "@/components/setup/SetupWizard";
import type { SetupConfigurationValues } from "@/components/setup/SetupConfigurationForm";
import { StudioLoadingScreen } from "@/components/StudioLoadingScreen";

type SetupGateProps = {
  children: React.ReactNode;
};

const SETUP_POLL_INTERVAL_MS = 2_000;
const SETUP_WAITING_POLL_INTERVAL_MS = 10_000;

export function setupPollingInterval(
  state: "checking" | "initializing" | "setup_required" | "ready" | undefined,
  isReconciling = false,
): number | false {
  if (isReconciling || state === "checking" || state === "initializing") {
    return SETUP_POLL_INTERVAL_MS;
  }
  return state === "setup_required" ? SETUP_WAITING_POLL_INTERVAL_MS : false;
}

export function setupPollingInBackground(): boolean {
  return true;
}

export function invalidateSetupStatus(queryClient: QueryClient): Promise<void> {
  return queryClient.invalidateQueries({
    queryKey: getGetSetupStatusQueryKey(),
  });
}

export function reconciliationMutationOptions(
  queryClient: QueryClient,
  mutationFn: () => Promise<unknown>,
): UseMutationOptions<unknown, Error, void, unknown> {
  return {
    mutationFn,
    onSettled: () => invalidateSetupStatus(queryClient),
  };
}

/**
 * After a step's GRANT statements run, show the re-verified report straight away,
 * then refetch the full status.
 */
export function applyGrantsCallbacks(
  queryClient: QueryClient,
): UseMutationOptions<
  AxiosResponse<SetupApplyGrantsResponse>,
  AxiosError<HTTPValidationError>,
  { stepId: SetupStepId }
> {
  return {
    onSuccess: (response) => {
      queryClient.setQueryData<AxiosResponse<SetupStatusResponse>>(
        getGetSetupStatusQueryKey(),
        (current) =>
          current && {
            ...current,
            data: { ...current.data, report: response.data.report },
          },
      );
    },
    onSettled: () => invalidateSetupStatus(queryClient),
  };
}

/**
 * Blocks Studio routes until the authenticated caller's setup readiness is
 * known. Remediation actions are rendered only from the backend report.
 */
export function SetupGate({ children }: SetupGateProps) {
  const queryClient = useQueryClient();
  const reconciliation = useMutation(
    reconciliationMutationOptions(queryClient, () => reconcileSetup()),
  );
  const override = useMutation({
    mutationFn: (stepId: SetupStepId) =>
      overrideSetupStep({ step_id: stepId }),
    onSettled: () => invalidateSetupStatus(queryClient),
  });
  const applyGrants = useApplySetupStepGrants({
    mutation: applyGrantsCallbacks(queryClient),
  });
  const [editingConfiguration, setEditingConfiguration] = useState(false);
  const configuration = useMutation({
    mutationFn: (values: SetupConfigurationValues) => configureSetup(values),
    // A successful save closes the form; the saved summary shows instead.
    onSuccess: () => setEditingConfiguration(false),
    onSettled: () => invalidateSetupStatus(queryClient),
  });
  const setupStatus = useQuery({
    queryKey: getGetSetupStatusQueryKey(),
    queryFn: () => getSetupStatus(),
    refetchInterval: (query) =>
      setupPollingInterval(
        query.state.data?.data.report.state,
        reconciliation.isPending ||
          override.isPending ||
          applyGrants.isPending,
      ),
    refetchIntervalInBackground: setupPollingInBackground(),
  });
  const [acknowledged, setAcknowledged] = useState(readAcknowledgedWarnings);
  const workspaceHost = useWorkspaceHost({
    query: {
      enabled:
        setupStatus.data?.data.can_manage === true &&
        setupStatus.data.data.report.state !== "ready",
    },
  });
  // Only list catalogs and groups while the configuration form shows.
  const listsWanted = configurationListsWanted(
    setupStatus.data?.data,
    acknowledged,
    editingConfiguration,
  );
  const catalogs = useListCatalogs({
    query: {
      enabled: listsWanted,
      retry: false,
      staleTime: 5 * 60_000,
    },
  });
  const groups = useListSetupGroups({
    query: {
      enabled: listsWanted,
      retry: false,
      staleTime: 5 * 60_000,
    },
  });
  // While readiness is being checked, show the same "Loading DQX Studio" spinner
  // AuthGuard uses, so the check reads as one continuous load — no flash of the
  // "Checking Studio readiness" card, and no blank/black screen.
  if (setupStatus.isPending) return <StudioLoadingScreen />;
  if (!setupStatus.data) return <SetupStatusUnavailable />;

  const view = setupView(
    setupStatus.data.data,
    acknowledged,
    editingConfiguration,
  );
  if (view.kind === "ready") return <>{children}</>;

  const grantStepId = applyGrants.variables?.stepId;
  let grantOutcome: StepGrantOutcome | undefined;
  if (grantStepId && applyGrants.isSuccess) {
    const outcome = grantRunOutcome(applyGrants.data.data);
    grantOutcome = outcome ? { stepId: grantStepId, outcome } : undefined;
  } else if (grantStepId && applyGrants.isError) {
    grantOutcome = {
      stepId: grantStepId,
      outcome: grantRequestFailure(setupErrorCode(applyGrants.error)),
    };
  }

  const onAcknowledgeWarnings = () =>
    setAcknowledged((current) =>
      acknowledgeWarnings(
        current,
        unacknowledgedWarnings(view.report, current).map(warningKey),
      ),
    );

  return (
    <SetupWizard
      view={view}
      workspaceHost={workspaceHost.data?.workspace_host}
      isReconciling={reconciliation.isPending}
      onReconcile={() => {
        // A fresh check replaces the outcome of the last GRANT run.
        applyGrants.reset();
        reconciliation.mutate();
      }}
      reconciliationFailed={reconciliation.isError || override.isError}
      onOverride={(stepId) => override.mutate(stepId)}
      isOverriding={override.isPending}
      onAcknowledgeWarnings={onAcknowledgeWarnings}
      onConfigure={(values) => configuration.mutate(values)}
      isConfiguring={configuration.isPending}
      configurationError={
        configuration.isError
          ? (setupErrorCode(configuration.error) ?? "default")
          : undefined
      }
      catalogList={setupOptionList({
        isPending: catalogs.isPending,
        isError: catalogs.isError,
        options: catalogs.data?.data.map((catalog) => catalog.name),
      })}
      groupList={setupOptionList({
        isPending: groups.isPending,
        isError: groups.isError,
        options: groups.data?.data.groups,
        truncated: groups.data?.data.truncated,
      })}
      onEditConfiguration={(editing) => {
        configuration.reset();
        setEditingConfiguration(editing);
      }}
      onApplyGrants={(stepId) => applyGrants.mutate({ stepId })}
      applyingGrantsStepId={applyGrants.isPending ? grantStepId : undefined}
      grantOutcome={grantOutcome}
    />
  );
}
