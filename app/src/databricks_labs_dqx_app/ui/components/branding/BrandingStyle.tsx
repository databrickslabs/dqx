import { useEffect } from "react";
import { useGetBranding, type BrandingOut } from "@/lib/api";
import { applyStyleSheet, snapshotFromApi, toStyleSheet, writeBrandingCache } from "@/lib/branding";
import selector from "@/lib/selector";

/** Applies the server's company theme and keeps the localStorage cache in sync. */
export function BrandingStyle() {
  const { data } = useGetBranding({
    query: { ...selector<BrandingOut>().query, staleTime: 60_000, retry: 2 },
  });
  useEffect(() => {
    if (!data) return;
    const snapshot = snapshotFromApi(data);
    applyStyleSheet(toStyleSheet(snapshot.overrides));
    // Always cache, including DQX Default, so the next load can skip the branding wait.
    writeBrandingCache(snapshot);
  }, [data]);
  return null;
}
