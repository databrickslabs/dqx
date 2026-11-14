import { createFileRoute, Navigate, redirect } from "@tanstack/react-router";
import { usePermissions } from "@/hooks/use-permissions";

interface ImportSearchParams {
  from?: string;
}

// Legacy URL — import now lives under Rules Registry. Keep this route as a
// permanent redirect so bookmarks and old sidebar links keep working.
export const Route = createFileRoute("/_sidebar/rules/import")({
  component: RulesImportRedirect,
  // ``?tab=contract`` / ``?tab=tables`` were the ODCS imports, which now live under Tables.
  beforeLoad: ({ location }) => {
    const { tab } = location.search as Record<string, unknown>;
    if (tab === "contract" || tab === "tables") {
      throw redirect({ to: "/monitored-tables/import", replace: true });
    }
  },
  validateSearch: (search: Record<string, unknown>): ImportSearchParams => ({
    from: typeof search.from === "string" ? search.from : undefined,
  }),
});

function RulesImportRedirect() {
  const { canCreateRules } = usePermissions();
  const { from } = Route.useSearch();
  // Preserve the old /rules/import page's canCreateRules guard on this legacy
  // route itself, rather than leaning on the destination page to re-check.
  // This closes a latent authorization bypass should that guard ever be
  // relaxed, and avoids a redirect flicker (an unauthorized user would
  // otherwise bounce through /registry-rules/import before being kicked out).
  // Unauthorized users land on /registry-rules — the same target the new
  // import page's guard uses.
  if (!canCreateRules) return <Navigate to="/registry-rules" replace />;
  return (
    <Navigate
      to="/registry-rules/import"
      search={{ from }}
      replace
    />
  );
}
