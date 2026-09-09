import { createFileRoute, Navigate } from "@tanstack/react-router";

// "Monitor a table" is now a modal (see AddMonitoredTableModal) launched
// directly from the Monitored Tables list, rather than a dedicated page.
// This route is kept as a redirect so old bookmarks/links to
// /monitored-tables/new keep working.
export const Route = createFileRoute("/_sidebar/monitored-tables/new")({
  component: () => <Navigate to="/monitored-tables" replace />,
});
