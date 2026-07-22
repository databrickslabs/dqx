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
   * Whether the user can trigger profiler and validation runs. True for
   * admins and rule authors (both hold the ``run_rules`` permission
   * directly). Approvers and viewers do NOT hold it — the orthogonal
   * RUNNER role has been removed; run access is now granted via the
   * ADMIN and RULE_AUTHOR roles directly.
   */
  canRunRules: boolean;
  isAdmin: boolean;
  isRuleApprover: boolean;
  isRuleAuthor: boolean;
  isViewer: boolean;
  /**
   * True iff the caller effectively holds run access (admin or
   * rule_author). Kept for backward-compat with code that reads
   * ``isRunner``; new code should prefer ``canRunRules``.
   */
  isRunner: boolean;
}

export function usePermissions(): UsePermissionsResult {
  // B2-22: the user's role is app-config that doesn't change during a session,
  // so pin it to staleTime: Infinity — it's fetched once and served from cache
  // on every mount (usePermissions runs on nearly every page) instead of
  // refetching per route/tab.
  const { data } = useCurrentUserRoleSuspense({
    query: { ...selector<UserRoleOut>().query, staleTime: Infinity },
  });

  return useMemo(() => {
    const role = data?.role ?? "viewer";
    const permissions = (data?.permissions ?? []) as Permission[];
    const isAdmin = role === "admin";
    // ``is_runner`` from the backend reflects run access (admin + rule_author).
    // OR with ``isAdmin`` defensively in case the field is missing (older
    // deployment, cached response, etc.). The orthogonal RUNNER role has been
    // removed — run_rules is now a permission of ADMIN and RULE_AUTHOR only.
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
      // backend (held by admin + rule_author roles). Fall back to isRunner
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
