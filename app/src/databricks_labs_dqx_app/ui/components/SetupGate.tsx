import { useEffect, useState } from "react";
import {
  useMutation,
  useQuery,
  useQueryClient,
  type QueryClient,
  type UseMutationOptions,
} from "@tanstack/react-query";

import {
  getGetSetupStatusQueryKey,
  getSetupStatus,
  reconcileSetup,
} from "@/lib/api";
import { setupView } from "@/lib/setup-state";
import { useWorkspaceHost } from "@/lib/api-custom";
import {
  SetupLoading,
  SetupStatusUnavailable,
  SetupWizard,
} from "@/components/setup/SetupWizard";

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
 * Returns true only once `active` has stayed true continuously for `delayMs`.
 * Used to suppress the setup loading screen during a fast readiness check so it
 * doesn't flash on every page load — the screen appears only if the check is
 * genuinely slow.
 */
function useDelayedFlag(active: boolean, delayMs: number): boolean {
  const [elapsed, setElapsed] = useState(false);
  useEffect(() => {
    if (!active) {
      setElapsed(false);
      return;
    }
    const timer = setTimeout(() => setElapsed(true), delayMs);
    return () => clearTimeout(timer);
  }, [active, delayMs]);
  return active && elapsed;
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
  const setupStatus = useQuery({
    queryKey: getGetSetupStatusQueryKey(),
    queryFn: () => getSetupStatus(),
    refetchInterval: (query) =>
      setupPollingInterval(
        query.state.data?.data.report.state,
        reconciliation.isPending,
      ),
    refetchIntervalInBackground: setupPollingInBackground(),
  });
  const workspaceHost = useWorkspaceHost({
    query: {
      enabled:
        setupStatus.data?.data.can_manage === true &&
        setupStatus.data.data.report.state !== "ready",
    },
  });
  // Don't flash the setup screen for a fast readiness check: while the query is
  // in flight, render nothing until it has been pending long enough (500ms) to
  // be worth a loading state. A ready Studio resolves well under that, so the
  // common path goes straight to the app with no flash.
  const showSetupLoading = useDelayedFlag(setupStatus.isPending, 500);
  if (setupStatus.isPending) return showSetupLoading ? <SetupLoading /> : null;
  if (!setupStatus.data) return <SetupStatusUnavailable />;

  const view = setupView(setupStatus.data.data);
  if (view.kind === "ready") return <>{children}</>;

  return (
    <SetupWizard
      view={view}
      workspaceHost={workspaceHost.data?.workspace_host}
      isReconciling={reconciliation.isPending}
      onReconcile={() => reconciliation.mutate()}
      reconciliationFailed={reconciliation.isError}
    />
  );
}
