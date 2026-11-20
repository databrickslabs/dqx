import type { GroupRowOut, TrendPointOut } from "@/lib/api";
import {
  groupTagOptions,
  selectionToTokens,
  toggleValue,
  valueSelected,
  type TagOption,
  type TagSelection,
} from "@/components/tags/tag-filter";

// Tags on the multi-table Results surfaces. The API returns by_tag /
// trend_by_tag rows for EVERY tag in scope, each carrying its tag id as
// tag_key: rule tags, plus on Global Results the tables' Unity Catalog
// catalog, schema and table tags and the column tags the rules map. The top
// tag filter decides which series the tag chart and box show: the picked
// values (a ticked key shows all of its values). With nothing picked the box
// shows every tag value and the chart shows none.
//
// A series is keyed by its filter token (see tag-filter.ts): `<id>=<value>`,
// or `!<id>` for the checks without the tag.

export type TagHistory = Record<string, Array<string | null>> | undefined;

/** Filter options from the scoped run history, including "Untagged" per tag. */
export function tagOptionsFromHistory(history: TagHistory): TagOption[] {
  return Object.entries(history ?? {}).flatMap(([id, values]) => values.map((value) => ({ id, value })));
}

export function tagSeriesToken(id: string, value: string | null): string {
  return value === null ? `!${id}` : `${id}=${value}`;
}

/** The series a chart point or box row belongs to; null when it carries no tag id. */
function seriesOf(tagKey: string | null | undefined, value: string | null | undefined): string | null {
  return tagKey ? tagSeriesToken(tagKey, value ?? null) : null;
}

/** True when the (tag id, value) series is shown under *selection*: every tagged value when nothing is picked. */
export function tagSeriesShown(selection: TagSelection, id: string, value: string | null): boolean {
  return selection.size === 0 ? value !== null : valueSelected(selection, id, value);
}

/** True when the series *token* is picked in *selection* (never with an empty selection). */
export function tagSeriesSelected(selection: TagSelection, token: string): boolean {
  const missing = token.startsWith("!");
  const body = missing ? token.slice(1) : token;
  const eq = body.indexOf("=");
  if (missing) return valueSelected(selection, body, null);
  return eq !== -1 && valueSelected(selection, body.slice(0, eq), body.slice(eq + 1));
}

/** The by_tag rows shown under *selection*, each labelled with its series token (unique across tags). */
export function shownTagGroups(groups: GroupRowOut[] | undefined, selection: TagSelection): GroupRowOut[] {
  return (groups ?? [])
    .filter((g) => g.tag_key && tagSeriesShown(selection, g.tag_key, g.label ?? null))
    .map((g) => ({ ...g, label: seriesOf(g.tag_key, g.label) }));
}

/**
 * The trend_by_tag points shown under *selection*, with *series* set to the
 * series token. Unlike the By tag box the chart shows nothing until tags are
 * picked: a line per value of every tag is too many to read.
 */
export function shownTagTrend(points: TrendPointOut[] | undefined, selection: TagSelection): TrendPointOut[] {
  if (selection.size === 0) return [];
  return (points ?? [])
    .filter((p) => p.tag_key && tagSeriesShown(selection, p.tag_key, p.series ?? null))
    .map((p) => ({ ...p, series: seriesOf(p.tag_key, p.series) }));
}

/** The filter tokens after clicking a series: toggles that tag value in the selection. */
export function toggleTagSeries(selection: TagSelection, history: TagHistory, token: string): string[] {
  const missing = token.startsWith("!");
  const body = missing ? token.slice(1) : token;
  const eq = body.indexOf("=");
  const id = missing || eq === -1 ? body : body.slice(0, eq);
  const group = groupTagOptions(tagOptionsFromHistory(history)).find((g) => g.id === id);
  if (!group) return selectionToTokens(selection);
  return selectionToTokens(toggleValue(selection, group, missing ? null : body.slice(eq + 1)));
}
