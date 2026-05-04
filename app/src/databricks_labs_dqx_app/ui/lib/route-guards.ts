import { redirect } from "@tanstack/react-router";
import type { QueryClient } from "@tanstack/react-query";
import {
  currentUserRole,
  getCurrentUserRoleQueryKey,
  type UserRoleOut,
} from "@/lib/api";

/**
 * Read the current user's role/permissions from the React Query cache,
 * fetching once and caching forever (the role is stable per session).
 *
 * Used by route ``beforeLoad`` guards which run *outside* React render
 * and therefore cannot use the suspense hook.
 *
 * IMPORTANT: this query key is *shared* with ``useCurrentUserRoleSuspense``
 * (Orval-generated). That hook caches the **full ``AxiosResponse``** under
 * this key and unwraps ``.data`` via a ``select`` selector at render
 * time. Our ``queryFn`` here MUST mirror that shape — otherwise whichever
 * caller writes the cache first leaves the other looking at the wrong
 * object. (A previous version of this guard returned ``resp.data``,
 * which caused legitimate runners to be redirected to ``/runs-history``
 * because the guard later read an ``AxiosResponse`` populated by the
 * sidebar's ``usePermissions`` hook and saw ``role === undefined``.)
 */
async function ensureUserRole(queryClient: QueryClient): Promise<UserRoleOut> {
  const queryKey = getCurrentUserRoleQueryKey();
  const cached = await queryClient.ensureQueryData({
    queryKey,
    queryFn: ({ signal }) => currentUserRole({ signal }),
    // Roles can change while the user is logged in (an admin reassigns
    // groups), so let React Query revalidate on a normal cadence rather
    // than caching forever. ``staleTime: 0`` means the cache is *used*
    // for instant nav but the next mount triggers a background refresh.
    staleTime: 60_000,
  });
  // Defensive unwrap: the cache normally holds the full AxiosResponse
  // (matching the generated hook), but tolerate older cache entries that
  // stored the payload directly so a hot-reloaded session doesn't break.
  const candidate = cached as unknown as { data?: UserRoleOut };
  if (
    candidate &&
    typeof candidate === "object" &&
    candidate.data &&
    typeof candidate.data === "object" &&
    "role" in candidate.data
  ) {
    return candidate.data;
  }
  return cached as unknown as UserRoleOut;
}

/**
 * Compute whether the user can see the Run Rules page.
 *
 * ``RUNNER`` is an additive role: admins are implicit runners, and any
 * other role becomes a runner only when their group has been mapped to
 * RUNNER explicitly. The backend returns ``is_runner`` and folds
 * ``run_rules`` into ``permissions``; we OR both for resilience.
 */
function canRunRulesFromRole(roleData: UserRoleOut | undefined): boolean {
  if (!roleData) return false;
  if (roleData.role === "admin") return true;
  if (roleData.is_runner === true) return true;
  return Array.isArray(roleData.permissions) && roleData.permissions.includes("run_rules");
}

interface BeforeLoadCtx {
  context: { queryClient: QueryClient };
}

/**
 * ``beforeLoad`` guard for routes that should only be reachable by users
 * with the orthogonal RUNNER role (admins included). Non-runners are
 * redirected to Runs History — which is universally readable — instead
 * of seeing a 403.
 *
 * Throwing ``redirect()`` from ``beforeLoad`` aborts the route load so
 * the protected route's component file is never imported, guaranteeing
 * URL-bar typing can't reach the page.
 */
export async function requireRunnerOrRedirect({ context }: BeforeLoadCtx) {
  const role = await ensureUserRole(context.queryClient);
  if (!canRunRulesFromRole(role)) {
    throw redirect({ to: "/runs-history" });
  }
}
