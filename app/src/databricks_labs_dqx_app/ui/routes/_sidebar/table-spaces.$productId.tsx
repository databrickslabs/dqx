import { createFileRoute, redirect } from "@tanstack/react-router";

// Renamed to Collections (bug-bash-v4 item 56) — redirect old
// /table-spaces/$productId deep links (preserving any ?tab=) to
// /collections/$productId.
export const Route = createFileRoute("/_sidebar/table-spaces/$productId")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  beforeLoad: ({ params, search }) => {
    throw redirect({
      to: "/collections/$productId" as string,
      params,
      search,
    });
  },
  component: () => null,
});
