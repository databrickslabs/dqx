import { createFileRoute, Navigate } from "@tanstack/react-router";
import { usePermissions } from "@/hooks/use-permissions";

// Legacy URL for contract-based rule generation. ODCS contracts are imported
// through Tables > Import to tables, which registers the contract's tables and
// applies the generated rules to them.
export const Route = createFileRoute("/_sidebar/rules/from-contract")({
  component: RulesFromContractRedirect,
});

function RulesFromContractRedirect() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/registry-rules" replace />;
  return <Navigate to="/monitored-tables/import" replace />;
}
