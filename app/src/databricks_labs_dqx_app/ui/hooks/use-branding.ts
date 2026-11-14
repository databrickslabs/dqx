import { useMemo } from "react";
import { useGetBranding, type BrandingOut } from "@/lib/api";
import { readBrandingCache, snapshotFromApi, type BrandingSnapshot } from "@/lib/branding";
import selector from "@/lib/selector";

/** Server branding when loaded, otherwise the locally cached snapshot (or null). */
export function useBranding(): BrandingSnapshot | null {
  const { data } = useGetBranding({
    query: { ...selector<BrandingOut>().query, staleTime: 60_000, retry: 2 },
  });
  return useMemo(() => (data ? snapshotFromApi(data) : readBrandingCache()), [data]);
}
