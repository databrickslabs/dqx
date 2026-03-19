import { useSuspenseQuery } from "@tanstack/react-query";
import {
  getCurrentUserQueryOptions,
  getCurrentUserRoleQueryOptions,
  getConfigQueryOptions,
} from "@/lib/api";

export function useCurrentUserSuspense(
  options?: Parameters<typeof getCurrentUserQueryOptions>[0],
) {
  return useSuspenseQuery(getCurrentUserQueryOptions(options));
}

export function useCurrentUserRoleSuspense(
  options?: Parameters<typeof getCurrentUserRoleQueryOptions>[0],
) {
  return useSuspenseQuery(getCurrentUserRoleQueryOptions(options));
}

export function useConfigSuspense(
  options?: Parameters<typeof getConfigQueryOptions>[0],
) {
  return useSuspenseQuery(getConfigQueryOptions(options));
}
