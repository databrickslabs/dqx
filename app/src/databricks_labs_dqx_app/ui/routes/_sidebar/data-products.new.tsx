import { createFileRoute, redirect } from "@tanstack/react-router";

// Renamed to Collections (bug-bash-v4 item 56) — redirect old
// /data-products/new bookmarks to /collections/new.
export const Route = createFileRoute("/_sidebar/data-products/new")({
  beforeLoad: () => {
    throw redirect({ to: "/collections/new" as string });
  },
  component: () => null,
});
