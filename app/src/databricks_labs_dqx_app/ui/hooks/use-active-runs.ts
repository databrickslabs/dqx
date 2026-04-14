import { useState, useCallback, useEffect } from "react";

const STORAGE_KEY = "dqx-active-runs";

export interface ActiveRun {
  run_id: string;
  job_run_id: number;
  view_fqn: string;
  table_fqn: string;
  submitted_at: number;
}

function readFromStorage(): ActiveRun[] {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as ActiveRun[];
    // Evict stale runs older than 2 hours
    const cutoff = Date.now() - 2 * 60 * 60 * 1000;
    return parsed.filter((r) => r.submitted_at > cutoff);
  } catch {
    return [];
  }
}

function writeToStorage(runs: ActiveRun[]) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(runs));
  } catch {
    // quota exceeded or unavailable — ignore
  }
}

export function useActiveRuns() {
  const [activeRuns, setActiveRuns] = useState<ActiveRun[]>(readFromStorage);

  useEffect(() => {
    writeToStorage(activeRuns);
  }, [activeRuns]);

  const addRuns = useCallback((runs: ActiveRun[]) => {
    setActiveRuns((prev) => {
      const ids = new Set(prev.map((r) => r.run_id));
      const newRuns = runs.filter((r) => !ids.has(r.run_id));
      return [...prev, ...newRuns];
    });
  }, []);

  const removeRun = useCallback((runId: string) => {
    setActiveRuns((prev) => prev.filter((r) => r.run_id !== runId));
  }, []);

  const clearAll = useCallback(() => {
    setActiveRuns([]);
  }, []);

  return { activeRuns, addRuns, removeRun, clearAll };
}
