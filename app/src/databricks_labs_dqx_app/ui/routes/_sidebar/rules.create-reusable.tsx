import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/rules/create-reusable")({
  beforeLoad: () => {
    throw redirect({ to: "/rules/create" });
  },
  component: () => null,
});
