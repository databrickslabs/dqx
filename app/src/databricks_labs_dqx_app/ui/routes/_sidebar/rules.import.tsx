import { createFileRoute, Navigate } from "@tanstack/react-router";
import { usePermissions } from "@/hooks/use-permissions";
import { coerceImportTab, type ImportTab } from "@/components/registry-rules/ImportRulesWorkspace";

interface ImportSearchParams {
  from?: string;
  tab?: ImportTab;
}

// Legacy URL — import now lives under Rules Registry. Keep this route as a
// permanent redirect so bookmarks and old sidebar links keep working.
export const Route = createFileRoute("/_sidebar/rules/import")({
  component: RulesImportRedirect,
  validateSearch: (search: Record<string, unknown>): ImportSearchParams => ({
    from: typeof search.from === "string" ? search.from : undefined,
    tab: coerceImportTab(search.tab),
  }),
});

function RulesImportRedirect() {
  const { canCreateRules } = usePermissions();
  const { tab, from } = Route.useSearch();
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
      search={{ tab, from }}
      replace
    />
  );
}
