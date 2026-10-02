import { createFileRoute, Navigate } from "@tanstack/react-router";
import { usePermissions } from "@/hooks/use-permissions";

// Table assignment belongs to Tables, while /registry-rules/import creates
// reusable templates only. Keep this legacy path for old bookmarks.
export const Route = createFileRoute("/_sidebar/registry-rules/bulk-import")({
  component: RegistryRulesBulkImportRedirect,
});

function RegistryRulesBulkImportRedirect() {
  const { canCreateRules } = usePermissions();
  // Enforce the destination's guard here too (mirrors the other legacy import
  // redirects): no authorization bypass if that guard is ever relaxed, and no
  // redirect flicker for unauthorized users.
  if (!canCreateRules) return <Navigate to="/registry-rules" replace />;
  return <Navigate to="/monitored-tables/import" replace />;
}
