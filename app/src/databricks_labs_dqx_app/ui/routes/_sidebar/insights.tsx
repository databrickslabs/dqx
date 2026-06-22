import { createFileRoute } from "@tanstack/react-router";

// The Insights dashboard is rendered by ``InsightsDashboardHost`` inside the
// persistent ``_sidebar`` layout rather than here, so the embedded Lakeview
// iframe survives navigation instead of reloading on every visit. This route
// exists only so ``/insights`` is navigable and matches the sidebar link; the
// layout shows the persistent host (and hides this empty Outlet) whenever the
// path starts with ``/insights``.
export const Route = createFileRoute("/_sidebar/insights")({
  component: () => null,
});
