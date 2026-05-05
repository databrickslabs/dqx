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
  | "manage_roles"
  | "run_rules";

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
  /**
   * Whether the user can see and use the "Run Rules" page. True for
   * admins (implicit) and any user with an explicit RUNNER role mapping.
   * Other primary roles (author, approver, viewer) do NOT grant this on
   * their own — RUNNER is orthogonal/additive.
   */
  canRunRules: boolean;
  isAdmin: boolean;
  isRuleApprover: boolean;
  isRuleAuthor: boolean;
  isViewer: boolean;
  /** True iff the orthogonal RUNNER role is held (admins always count). */
  isRunner: boolean;
}

export function usePermissions(): UsePermissionsResult {
  const { data } = useCurrentUserRoleSuspense(selector<UserRoleOut>());

  return useMemo(() => {
    const role = data?.role ?? "viewer";
    const permissions = (data?.permissions ?? []) as Permission[];
    const isAdmin = role === "admin";
    // ``is_runner`` from the backend already includes the admin-implicit
    // case, but we OR with ``isAdmin`` defensively in case the field is
    // missing (older deployment, cached response, etc.).
    const isRunner = (data?.is_runner ?? false) || isAdmin;

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
      // Prefer the explicit ``run_rules`` permission flag from the
      // backend (which folds in admin + runner). Fall back to isRunner
      // for resilience against older API responses.
      canRunRules: hasPermission("run_rules") || isRunner,
      isAdmin,
      isRuleApprover: role === "rule_approver",
      isRuleAuthor: role === "rule_author",
      isViewer: role === "viewer",
      isRunner,
    };
  }, [data]);
}
