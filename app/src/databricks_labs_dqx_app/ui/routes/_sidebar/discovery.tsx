import { createFileRoute, redirect } from "@tanstack/react-router";

// The catalog Discovery browser was folded into Monitored Tables (nav-
// consolidation cleanup, Phase 5) — registering a table there gives the
// same catalog/schema/table browse plus profiling and rule application
// in one place. Old bookmarks/links to ``/discovery`` redirect there
// rather than 404ing.
export const Route = createFileRoute("/_sidebar/discovery")({
  beforeLoad: () => {
    throw redirect({ to: "/monitored-tables" as string });
  },
  component: () => null,
});
