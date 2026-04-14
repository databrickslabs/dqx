import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/rules/create")({
  beforeLoad: () => {
    throw redirect({ to: "/rules/generate" });
  },
  component: () => null,
});
