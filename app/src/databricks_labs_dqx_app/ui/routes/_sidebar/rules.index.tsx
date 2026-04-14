import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/rules/")({
  beforeLoad: () => {
    throw redirect({ to: "/rules/active" as string });
  },
  component: () => null,
});
