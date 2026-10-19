/** Choices offered by a searchable picker in the setup configuration form (catalogs, groups). */
export type SetupOptionList =
  | { status: "loading" }
  | { status: "ready"; options: string[] }
  /** Listing failed or found nothing: the form falls back to a text input. */
  | { status: "unavailable" };

/** Most options rendered at once; typing narrows the rest, so huge lists stay responsive. */
export const MAX_VISIBLE_OPTIONS = 100;

/** Map a setup listing query onto what its form field should render. */
export function setupOptionList(query: {
  isPending: boolean;
  isError: boolean;
  options?: readonly string[];
}): SetupOptionList {
  if (query.isError) return { status: "unavailable" };
  if (query.isPending) return { status: "loading" };
  const options = [...(query.options ?? [])];
  return options.length > 0
    ? { status: "ready", options }
    : { status: "unavailable" };
}

/**
 * Options matching *search* (case-insensitive substring), plus the typed name
 * itself when it isn't a listed option, so a name the list doesn't show can
 * still be chosen.
 */
export function optionMatches(
  options: readonly string[],
  search: string,
): { matches: string[]; typed: string | null } {
  const query = search.trim();
  const needle = query.toLowerCase();
  const matches = (
    needle
      ? options.filter((option) => option.toLowerCase().includes(needle))
      : [...options]
  ).slice(0, MAX_VISIBLE_OPTIONS);
  const listed = options.some((option) => option.toLowerCase() === needle);
  return { matches, typed: query && !listed ? query : null };
}
