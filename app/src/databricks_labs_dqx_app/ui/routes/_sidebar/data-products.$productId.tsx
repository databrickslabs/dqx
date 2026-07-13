import { createFileRoute, redirect } from "@tanstack/react-router";

// Renamed to Table Spaces (P21 item 28) — redirect old
// /data-products/$productId deep links (preserving any ?tab=) to
// /table-spaces/$productId.
export const Route = createFileRoute("/_sidebar/data-products/$productId")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  beforeLoad: ({ params, search }) => {
    throw redirect({
      to: "/table-spaces/$productId" as string,
      params,
      search,
    });
  },
  component: () => null,
});
