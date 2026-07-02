import { createFileRoute, redirect } from "@tanstack/react-router";

// The Active Rules library (materialized rules grouped by target table)
// was superseded by Monitored Tables, which shows the same applied-rule
// state per table plus profiling and apply/publish actions (nav-
// consolidation cleanup, Phase 5). Old bookmarks/links to
// ``/rules/active`` redirect there rather than 404ing.
export const Route = createFileRoute("/_sidebar/rules/active")({
  beforeLoad: () => {
    throw redirect({ to: "/monitored-tables" as string });
  },
  component: () => null,
});
