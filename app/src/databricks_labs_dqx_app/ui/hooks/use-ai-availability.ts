import { useCallback, useState } from "react";
import { usePermissions } from "@/hooks/use-permissions";
import { useGetAiSettings } from "@/lib/api";

/**
 * Shared "is AI usable right now" gate for every AI affordance in the app
 * (Rules Registry Phase 4D — Build-with-AI, per-field suggest, mapping
 * suggester). `GET /config/ai-settings` is ADMIN-only, so non-admins can't
 * proactively check the kill-switch — for them we optimistically show the
 * affordance and degrade on the first 503 (`AIUnavailableError`) instead.
 *
 * Every consumer must call `reportUnavailable(reason)` from the catch
 * branch of an AI call when the response status is 503, so the UI hides
 * itself cleanly instead of repeatedly failing.
 */
export interface AiAvailability {
  /** Whether AI affordances should render at all. */
  available: boolean;
  /** Human-readable reason surfaced by the backend, if AI is known-unavailable. */
  reason: string | null;
  /** Call after a 503 (`AIUnavailableError`) response to hide AI affordances for the rest of this view. */
  reportUnavailable: (reason: string) => void;
}

export function useAiAvailability(): AiAvailability {
  const perms = usePermissions();
  const { data } = useGetAiSettings({
    query: { enabled: perms.isAdmin, retry: false },
  });
  const [runtimeReason, setRuntimeReason] = useState<string | null>(null);

  const reportUnavailable = useCallback((reason: string) => {
    setRuntimeReason(reason);
  }, []);

  const knownDisabled = perms.isAdmin && data?.data?.ai_enabled === false;

  return {
    available: perms.canGenerateRules && !knownDisabled && runtimeReason === null,
    reason: runtimeReason,
    reportUnavailable,
  };
}

/** Extracts a clean reason string from an AI-call error, distinguishing a 503 kill-switch response. */
export function aiUnavailableReason(err: unknown): string | null {
  const axErr = err as { response?: { status?: number; data?: { detail?: string } } };
  if (axErr?.response?.status === 503) {
    return axErr.response?.data?.detail ?? "AI is currently unavailable.";
  }
  return null;
}
