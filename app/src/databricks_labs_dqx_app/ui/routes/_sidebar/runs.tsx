import { createFileRoute, redirect } from "@tanstack/react-router";

// "Run Rules" (execute-now + schedules) was folded into Data Products
// (Phase 11) — grouping monitored tables into governed, versioned,
// schedulable bundles replaces the old flat run/schedule editor. Old
// bookmarks/links to ``/runs`` (and any ``?tab=`` search params it used
// to carry) redirect to the new list rather than 404ing, following the
// same Phase-5 redirect pattern used by ``discovery.tsx`` and
// ``monitored-tables.new.tsx``. ``/runs/$runName`` and ``/runs/`` are
// separate route files and keep resolving on their own.
export const Route = createFileRoute("/_sidebar/runs")({
  beforeLoad: () => {
    throw redirect({ to: "/collections" as string });
  },
  component: () => null,
});
