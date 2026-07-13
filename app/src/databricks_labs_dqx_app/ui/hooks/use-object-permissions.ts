/**
 * Thin wrapper around `useGetEffectivePermissions` for gating action buttons
 * on a securable object (rule, monitored table, table space). Returns the
 * caller's `EffectivePermissionsOut` (or undefined while loading).
 */
import { useGetEffectivePermissions, type EffectivePermissionsOut } from "@/lib/api";

export function useObjectPermissions(
  objectType: "registry_rule" | "monitored_table" | "data_product",
  objectId: string,
): EffectivePermissionsOut | undefined {
  const { data } = useGetEffectivePermissions(objectType, objectId, {
    query: {
      select: (d) => d.data,
      enabled: !!objectId,
    },
  });
  return data;
}
