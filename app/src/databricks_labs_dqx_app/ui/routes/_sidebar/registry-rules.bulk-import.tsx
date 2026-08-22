import { createFileRoute, Navigate } from "@tanstack/react-router";
import { usePermissions } from "@/hooks/use-permissions";

// Bulk contract import used to be its own page, reachable only from a card at
// the bottom of the import page — easy to miss and easy to mistake for a
// variant of the "From data contract" tab. It is now the "Import to tables" tab
// of ``/registry-rules/import``, so this route stays only as a redirect for
// existing bookmarks.
export const Route = createFileRoute("/_sidebar/registry-rules/bulk-import")({
  component: RegistryRulesBulkImportRedirect,
});

function RegistryRulesBulkImportRedirect() {
  const { canCreateRules } = usePermissions();
  // Enforce the destination's guard here too (mirrors the other legacy import
  // redirects): no authorization bypass if that guard is ever relaxed, and no
  // redirect flicker for unauthorized users.
  if (!canCreateRules) return <Navigate to="/registry-rules" replace />;
  return <Navigate to="/registry-rules/import" search={{ tab: "tables" }} replace />;
}
