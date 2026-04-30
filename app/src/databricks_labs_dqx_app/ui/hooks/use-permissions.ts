import { useMemo } from "react";
import { useCurrentUserRoleSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import type { UserRoleOut } from "@/lib/api";

export type Permission =
  | "view_rules"
  | "create_rules"
  | "edit_rules"
  | "generate_rules"
  | "submit_rules"
  | "approve_rules"
  | "export_rules"
  | "configure_storage"
  | "manage_roles";

export interface UsePermissionsResult {
  role: string;
  permissions: Permission[];
  hasPermission: (permission: Permission) => boolean;
  canViewRules: boolean;
  canCreateRules: boolean;
  canEditRules: boolean;
  canGenerateRules: boolean;
  canSubmitRules: boolean;
  canApproveRules: boolean;
  canExportRules: boolean;
  canConfigureStorage: boolean;
  canManageRoles: boolean;
  isAdmin: boolean;
  isRuleApprover: boolean;
  isRuleAuthor: boolean;
  isViewer: boolean;
}

export function usePermissions(): UsePermissionsResult {
  const { data } = useCurrentUserRoleSuspense(selector<UserRoleOut>());

  return useMemo(() => {
    const role = data?.role ?? "viewer";
    const permissions = (data?.permissions ?? []) as Permission[];

    const hasPermission = (permission: Permission): boolean =>
      permissions.includes(permission);

    return {
      role,
      permissions,
      hasPermission,
      canViewRules: hasPermission("view_rules"),
      canCreateRules: hasPermission("create_rules"),
      canEditRules: hasPermission("edit_rules"),
      canGenerateRules: hasPermission("generate_rules"),
      canSubmitRules: hasPermission("submit_rules"),
      canApproveRules: hasPermission("approve_rules"),
      canExportRules: hasPermission("export_rules"),
      canConfigureStorage: hasPermission("configure_storage"),
      canManageRoles: hasPermission("manage_roles"),
      isAdmin: role === "admin",
      isRuleApprover: role === "rule_approver",
      isRuleAuthor: role === "rule_author",
      isViewer: role === "viewer",
    };
  }, [data]);
}
