import type * as React from "react";
import { Loader2 } from "lucide-react";

/**
 * "A run is in progress" banner, ported from dqlake's
 * `components/bindings/RunInProgressBanner.tsx`. Renders nothing unless
 * `show` is true. Deviation from dqlake: no default English children —
 * callers pass the translated message (this repo's i18n mandate), and the
 * activity signal comes from the detail page's ACTIVE-RUN-SCOPED poll (a
 * run triggered from that page), never an idle poll.
 */
export function RunInProgressBanner({
  show,
  children,
}: {
  show: boolean;
  children: React.ReactNode;
}) {
  if (!show) return null;
  return (
    <div
      role="status"
      className="rounded border border-sky-500/40 bg-sky-500/5 px-3 py-2 text-xs flex items-center gap-2"
    >
      <Loader2 className="h-3.5 w-3.5 animate-spin text-sky-500" />
      {children}
    </div>
  );
}
