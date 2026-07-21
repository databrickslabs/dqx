import { createFileRoute, redirect } from "@tanstack/react-router";

// "Table Spaces" was renamed to "Collections" (bug-bash-v4 item 56). The
// canonical route path moved from /table-spaces to /collections; this stub
// redirects old bookmarks/links following the established redirect pattern
// (see data-products.index.tsx / runs.tsx). Backend API paths, DB table
// names, and internal identifiers deliberately keep the data-products
// spelling.
export const Route = createFileRoute("/_sidebar/table-spaces/")({
  beforeLoad: () => {
    throw redirect({ to: "/collections" as string });
  },
  component: () => null,
});
