import { createFileRoute, redirect } from "@tanstack/react-router";

// Renamed to Table Spaces (P21 item 28) — redirect old /data-products/new
// bookmarks to /table-spaces/new.
export const Route = createFileRoute("/_sidebar/data-products/new")({
  beforeLoad: () => {
    throw redirect({ to: "/table-spaces/new" as string });
  },
  component: () => null,
});
