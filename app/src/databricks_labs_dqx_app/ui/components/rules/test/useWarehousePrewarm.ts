import { useEffect, useRef, useState } from "react";
import { usePrewarmRuleTestWarehouse } from "@/lib/api";

/**
 * Pre-warms the configured SQL warehouse when the Test tab mounts (P22-E,
 * ported from dqlake's `test/useWarehousePrewarm.ts`). On mount it fires a
 * start-if-stopped request, then polls status every 3s until the warehouse is
 * running. Polling stops once `running` is true OR the request errors (e.g. a
 * 401 with no OBO in local dev, or a 503 with no configured warehouse).
 *
 * Returns `ready` (running || errored). When errored we treat the warehouse as
 * "ready" so the Run-test button isn't stuck behind a "Waiting for Warehouse"
 * tooltip forever — the actual test run will surface any real error.
 */
export function useWarehousePrewarm() {
  const [running, setRunning] = useState(false);
  const [errored, setErrored] = useState(false);

  const prewarm = usePrewarmRuleTestWarehouse({
    mutation: {
      onSuccess: (res) => setRunning(res.data.running),
      onError: () => setErrored(true),
    },
  });

  // Keep a stable ref to mutate so the effects below don't re-fire every render.
  const mutateRef = useRef(prewarm.mutate);
  mutateRef.current = prewarm.mutate;

  // Mount-once: kick a start-if-stopped. Idempotent across mounts by design.
  useEffect(() => {
    mutateRef.current({ data: { start: true } });
  }, []);

  // Poll status while not yet running and not errored; clear once either holds.
  const ready = running || errored;
  useEffect(() => {
    if (ready) return;
    const id = window.setInterval(() => {
      mutateRef.current({ data: { start: false } });
    }, 3000);
    return () => window.clearInterval(id);
  }, [ready]);

  return { ready, warming: !ready };
}
