import { createFileRoute, redirect } from "@tanstack/react-router";

// ``/rules/active`` itself now redirects to Monitored Tables (nav-
// consolidation cleanup, Phase 5); redirect straight there to avoid a
// double hop.
export const Route = createFileRoute("/_sidebar/rules/")({
  beforeLoad: () => {
    throw redirect({ to: "/monitored-tables" as string });
  },
  component: () => null,
});
