import { createFileRoute, redirect } from "@tanstack/react-router";

// The single-table rule editor was folded into the Rules Registry's
// authoring modal (DQX Native / Low-Code / SQL toggle) as part of the
// nav-consolidation cleanup (Phase 5). Old bookmarks/links to
// ``/rules/single-table`` (with or without an ``fqn``/``ruleId`` query
// string) redirect to the registry rather than 404ing.
export const Route = createFileRoute("/_sidebar/rules/single-table")({
  beforeLoad: () => {
    throw redirect({ to: "/registry-rules" as string });
  },
  component: () => null,
});
