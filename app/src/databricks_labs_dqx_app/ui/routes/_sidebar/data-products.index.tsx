import { createFileRoute, redirect } from "@tanstack/react-router";

// "Data Products" was renamed (now "Collections", bug-bash-v4 item 56). The
// canonical route path is /collections; this stub redirects old
// bookmarks/links following the established redirect pattern (see runs.tsx).
// Backend API paths, DB table names, and internal identifiers deliberately
// keep the data-products spelling.
export const Route = createFileRoute("/_sidebar/data-products/")({
  beforeLoad: () => {
    throw redirect({ to: "/collections" as string });
  },
  component: () => null,
});
