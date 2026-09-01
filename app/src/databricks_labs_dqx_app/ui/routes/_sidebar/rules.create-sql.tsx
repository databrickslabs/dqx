import { createFileRoute, redirect } from "@tanstack/react-router";

// The cross-table SQL rule editor was folded into the Rules Registry's
// authoring modal (SQL type) as part of the nav-consolidation cleanup
// (Phase 5). Old bookmarks/links to ``/rules/create-sql`` redirect to
// the registry rather than 404ing.
export const Route = createFileRoute("/_sidebar/rules/create-sql")({
  beforeLoad: () => {
    throw redirect({ to: "/registry-rules" as string });
  },
  component: () => null,
});
