import { useEffect } from "react";
import { useGetBranding, type BrandingOut } from "@/lib/api";
import {
  applyStyleSheet,
  clearBrandingCache,
  snapshotFromApi,
  toStyleSheet,
  writeBrandingCache,
} from "@/lib/branding";
import selector from "@/lib/selector";

/** Applies the server's company theme and keeps the localStorage cache in sync. */
export function BrandingStyle() {
  const { data } = useGetBranding({
    query: { ...selector<BrandingOut>().query, staleTime: 60_000, retry: 2 },
  });
  useEffect(() => {
    if (!data) return;
    const snapshot = snapshotFromApi(data);
    const css = toStyleSheet(snapshot.overrides);
    applyStyleSheet(css);
    const customised = !!css || !!snapshot.companyName || !!snapshot.logos.light || !!snapshot.logos.dark;
    if (customised) writeBrandingCache(snapshot);
    else clearBrandingCache();
  }, [data]);
  return null;
}
