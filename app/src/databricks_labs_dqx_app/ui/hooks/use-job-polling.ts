import { useState, useEffect, useCallback, useRef } from "react";

export type JobState = "PENDING" | "RUNNING" | "TERMINATED" | "SKIPPED" | "INTERNAL_ERROR";
export type ResultState = "SUCCESS" | "FAILED" | "TIMEDOUT" | "CANCELED" | null;

interface JobStatus {
  run_id: string;
  state: string;
  result_state?: string | null;
  message?: string | null;
}

interface UseJobPollingOptions {
  /** Function to fetch the current status */
  fetchStatus: () => Promise<JobStatus>;
  /** Called when the job reaches a terminal state */
  onComplete?: (status: JobStatus) => void;
  /** Called when polling encounters an error */
  onError?: (error: Error) => void;
  /** Polling interval in ms (default: 3000) */
  interval?: number;
  /** Whether polling is enabled (default: false) */
  enabled?: boolean;
}

interface UseJobPollingResult {
  status: JobStatus | null;
  isPolling: boolean;
  isTerminated: boolean;
  isSuccess: boolean;
  isFailed: boolean;
  error: Error | null;
  startPolling: () => void;
  stopPolling: () => void;
}

const TERMINAL_STATES = new Set(["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]);

export function useJobPolling({
  fetchStatus,
  onComplete,
  onError,
  interval = 3000,
  enabled = false,
}: UseJobPollingOptions): UseJobPollingResult {
  const [status, setStatus] = useState<JobStatus | null>(null);
  const [isPolling, setIsPolling] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const onCompleteRef = useRef(onComplete);
  const onErrorRef = useRef(onError);
  const fetchStatusRef = useRef(fetchStatus);

  // Keep refs up to date
  onCompleteRef.current = onComplete;
  onErrorRef.current = onError;
  fetchStatusRef.current = fetchStatus;

  const stopPolling = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    setIsPolling(false);
  }, []);

  const poll = useCallback(async () => {
    try {
      const result = await fetchStatusRef.current();
      setStatus(result);
      setError(null);

      if (TERMINAL_STATES.has(result.state)) {
        stopPolling();
        onCompleteRef.current?.(result);
      }
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      setError(e);
      onErrorRef.current?.(e);
    }
  }, [stopPolling]);

  const startPolling = useCallback(() => {
    setIsPolling(true);
    setError(null);
    // Poll immediately, then on interval
    poll();
    intervalRef.current = setInterval(poll, interval);
  }, [poll, interval]);

  // Auto-start/stop when enabled changes
  useEffect(() => {
    if (enabled && !isPolling) {
      startPolling();
    } else if (!enabled && isPolling) {
      stopPolling();
    }
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [enabled]); // eslint-disable-line react-hooks/exhaustive-deps

  const isTerminated = status ? TERMINAL_STATES.has(status.state) : false;
  const isSuccess = isTerminated && status?.result_state === "SUCCESS";
  const isFailed = isTerminated && status?.result_state !== "SUCCESS";

  return {
    status,
    isPolling,
    isTerminated,
    isSuccess,
    isFailed,
    error,
    startPolling,
    stopPolling,
  };
}
