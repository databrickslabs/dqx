import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/rules/create-reusable")({
  beforeLoad: () => {
    throw redirect({ to: "/rules/generate" });
  },
  component: () => null,
});
