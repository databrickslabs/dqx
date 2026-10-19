/** Catalogs offered by the setup configuration form's catalog picker. */
export type SetupCatalogList =
  | { status: "loading" }
  | { status: "ready"; catalogs: string[] }
  /** Listing failed or found nothing: the form falls back to a text input. */
  | { status: "unavailable" };

/** Most catalogs rendered at once; typing narrows the rest, so huge lists stay responsive. */
export const MAX_VISIBLE_CATALOGS = 100;

/** Map the setup catalog query onto what the catalog field should render. */
export function setupCatalogList(query: {
  isPending: boolean;
  isError: boolean;
  catalogs?: readonly string[];
}): SetupCatalogList {
  if (query.isError) return { status: "unavailable" };
  if (query.isPending) return { status: "loading" };
  const catalogs = [...(query.catalogs ?? [])];
  return catalogs.length > 0
    ? { status: "ready", catalogs }
    : { status: "unavailable" };
}

/**
 * Catalogs matching *search* (case-insensitive substring), plus the typed name
 * itself when it isn't a listed catalog, so a catalog the list doesn't show can
 * still be chosen.
 */
export function catalogMatches(
  catalogs: readonly string[],
  search: string,
): { matches: string[]; typed: string | null } {
  const query = search.trim();
  const needle = query.toLowerCase();
  const matches = (
    needle
      ? catalogs.filter((catalog) => catalog.toLowerCase().includes(needle))
      : [...catalogs]
  ).slice(0, MAX_VISIBLE_CATALOGS);
  const listed = catalogs.some((catalog) => catalog.toLowerCase() === needle);
  return { matches, typed: query && !listed ? query : null };
}
