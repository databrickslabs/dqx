import { createFileRoute, redirect } from "@tanstack/react-router";

// "Data Products" was renamed to "Table Spaces" (P21 item 28). The route
// path moved from /data-products to /table-spaces; this stub redirects old
// bookmarks/links following the established Phase-5 redirect pattern (see
// runs.tsx). Backend API paths, DB table names, and internal identifiers
// deliberately keep the data-products spelling.
export const Route = createFileRoute("/_sidebar/data-products/")({
  beforeLoad: () => {
    throw redirect({ to: "/table-spaces" as string });
  },
  component: () => null,
});
