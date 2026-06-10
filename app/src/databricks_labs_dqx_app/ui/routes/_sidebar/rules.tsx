import { createFileRoute, Outlet } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/rules")({
  component: RulesLayout,
});

function RulesLayout() {
  return <Outlet />;
}
