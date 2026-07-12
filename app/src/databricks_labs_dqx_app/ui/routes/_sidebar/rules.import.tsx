import { createFileRoute, Navigate } from "@tanstack/react-router";
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
  const { tab, from } = Route.useSearch();
  return (
    <Navigate
      to="/registry-rules/import"
      search={{ tab, from }}
      replace
    />
  );
}
