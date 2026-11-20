// Shared model of the tag filter used on the Rules, Tables, Collections and
// Results pages.
//
// A tag id is `<source>:<key>` (mirrors backend/tag_ids.py). The source says
// where the tag lives: a rule (Studio tags), a Unity Catalog catalog, schema
// or table, or a column. A column's tags are the Unity Catalog tags its rule
// ties to the column it checks, never the column's own Unity Catalog tags.
//
// A selection maps each tag id to the values picked under it. An empty set
// means "any value of this tag" (what ticking the key does); `null` selects
// the items WITHOUT the tag (Results' "Untagged"). Selections of the same tag
// id are ORed and different tag ids are ANDed: domain = finance or customer,
// and pii = true.

export type TagSource = "catalog" | "schema" | "table" | "column" | "rule";

/** Source order in the dropdown: rule tags first, then column up to catalog. */
export const TAG_SOURCES: readonly TagSource[] = ["rule", "column", "table", "schema", "catalog"];

export function tagId(source: TagSource, key: string): string {
  return `${source}:${key}`;
}

/** Split a tag id into its source and key; null when the source is unknown or the key empty. */
export function splitTagId(id: string): { source: TagSource; key: string } | null {
  const i = id.indexOf(":");
  if (i <= 0) return null;
  const source = id.slice(0, i) as TagSource;
  const key = id.slice(i + 1);
  return TAG_SOURCES.includes(source) && key ? { source, key } : null;
}

/** One pickable value of a tag. `""` is a key-only tag; null means "without this tag". */
export interface TagOption {
  id: string;
  value: string | null;
}

export type TagSelection = ReadonlyMap<string, ReadonlySet<string | null>>;

export const EMPTY_TAG_SELECTION: TagSelection = new Map();

/** The tags an item carries: tag id -> its values (one per id on a check, several on a table). */
export type ItemTags = ReadonlyMap<string, ReadonlySet<string>>;

/** Collect *[id, value]* pairs into {@link ItemTags}. */
export function itemTags(pairs: Iterable<readonly [string, string]>): ItemTags {
  const out = new Map<string, Set<string>>();
  for (const [id, value] of pairs) {
    let values = out.get(id);
    if (!values) out.set(id, (values = new Set()));
    values.add(value);
  }
  return out;
}

/** True when *tags* satisfies every selected tag id (see the module comment). */
export function tagsMatchSelection(tags: ItemTags, selection: TagSelection): boolean {
  for (const [id, wanted] of selection) {
    const have = tags.get(id);
    if (wanted.size === 0) {
      if (!have) return false;
      continue;
    }
    const ok = [...wanted].some((value) => (value === null ? !have : (have?.has(value) ?? false)));
    if (!ok) return false;
  }
  return true;
}

// ── Token form ─────────────────────────────────────────────────────────────
// Saved views and the Results `tag` API param store a selection as tokens:
// `<id>` any value, `<id>=<value>` one value, `!<id>` without the tag.

export function selectionToTokens(selection: TagSelection): string[] {
  const out: string[] = [];
  for (const [id, values] of selection) {
    if (values.size === 0) out.push(id);
    for (const value of values) out.push(value === null ? `!${id}` : `${id}=${value}`);
  }
  return out.sort();
}

/** Inverse of {@link selectionToTokens}. Tokens without a valid tag id are dropped. */
export function selectionFromTokens(tokens: readonly string[]): TagSelection {
  const out = new Map<string, Set<string | null>>();
  for (const token of tokens) {
    const missing = token.startsWith("!");
    const body = missing ? token.slice(1) : token;
    const eq = body.indexOf("=");
    const id = eq === -1 ? body : body.slice(0, eq);
    if (!splitTagId(id) || (missing && eq !== -1)) continue;
    let values = out.get(id);
    if (!values) out.set(id, (values = new Set()));
    if (missing) values.add(null);
    else if (eq !== -1) values.add(body.slice(eq + 1));
  }
  // A key token ("any value") wins over value tokens of the same id.
  for (const token of tokens) if (out.has(token)) out.set(token, new Set());
  return out;
}

/** Every token a selection over *options* could hold, for saved-view sanitising. */
export function tokensOfOptions(options: readonly TagOption[]): string[] {
  const out = new Set<string>();
  for (const { id, value } of options) {
    out.add(id);
    out.add(value === null ? `!${id}` : `${id}=${value}`);
  }
  return [...out];
}

// ── Dropdown grouping and selection edits ──────────────────────────────────

/** One tag key in the dropdown, with its values (sorted, "without this tag" last). */
export interface TagKeyGroup {
  id: string;
  source: TagSource;
  key: string;
  values: (string | null)[];
}

/** Group *options* by tag id, ordered by source (see {@link TAG_SOURCES}) then key. */
export function groupTagOptions(options: readonly TagOption[]): TagKeyGroup[] {
  const byId = new Map<string, Set<string | null>>();
  for (const { id, value } of options) {
    if (!splitTagId(id)) continue;
    let values = byId.get(id);
    if (!values) byId.set(id, (values = new Set()));
    values.add(value);
  }
  const groups: TagKeyGroup[] = [];
  for (const [id, values] of byId) {
    const { source, key } = splitTagId(id)!;
    const present = [...values].filter((v): v is string => v !== null).sort((a, b) => a.localeCompare(b));
    groups.push({ id, source, key, values: values.has(null) ? [...present, null] : present });
  }
  return groups.sort(
    (a, b) => TAG_SOURCES.indexOf(a.source) - TAG_SOURCES.indexOf(b.source) || a.key.localeCompare(b.key),
  );
}

/** True when a key has values to drill into; a key-only tag (value "") is a leaf. */
export function hasValuesBelow(group: TagKeyGroup): boolean {
  return group.values.some((v) => v !== null && v !== "");
}

/** Display names per source, e.g. "Table tag" ("tag" is part of the name). */
export type TagSourceNames = Record<TagSource, string>;

export interface TagLabels {
  untagged: string;
  source: TagSourceNames;
}

/**
 * Display form of a filter or series token: "Table tag · layer: gold",
 * "Rule tag · domain: Untagged", or "Column tag · class.location" for a
 * whole-key ("any value") token or a key-only tag.
 */
export function tagTokenLabel(token: string, labels: TagLabels): string {
  const missing = token.startsWith("!");
  const body = missing ? token.slice(1) : token;
  const eq = body.indexOf("=");
  const id = missing || eq === -1 ? body : body.slice(0, eq);
  const parsed = splitTagId(id);
  if (!parsed) return token;
  const name = `${labels.source[parsed.source]} · ${parsed.key}`;
  if (missing) return `${name}: ${labels.untagged}`;
  const value = eq === -1 ? "" : body.slice(eq + 1);
  return value === "" ? name : `${name}: ${value}`;
}

/** Keep groups whose key, or one of whose values, contains *query* (case-insensitive). */
export function filterTagGroups(groups: readonly TagKeyGroup[], query: string): TagKeyGroup[] {
  const q = query.trim().toLowerCase();
  if (!q) return [...groups];
  return groups.filter(
    (g) => g.key.toLowerCase().includes(q) || g.values.some((v) => v !== null && v.toLowerCase().includes(q)),
  );
}

/** Whether a key is unselected, narrowed to some values, or fully selected (any value). */
export function keyState(selection: TagSelection, id: string): "none" | "some" | "all" {
  const values = selection.get(id);
  if (!values) return "none";
  return values.size === 0 ? "all" : "some";
}

/** True when *value* of *id* is selected, directly or because the whole key is. */
export function valueSelected(selection: TagSelection, id: string, value: string | null): boolean {
  const values = selection.get(id);
  if (!values) return false;
  return values.size === 0 ? value !== null : values.has(value);
}

/** Ticking a key selects any value of it; ticking a fully selected key clears it. */
export function toggleKey(selection: TagSelection, id: string): TagSelection {
  const next = new Map(selection);
  if (keyState(selection, id) === "all") next.delete(id);
  else next.set(id, new Set());
  return next;
}

/**
 * Toggle one value of *group*. Unticking a value of a fully selected key keeps
 * every other value; a key with no values left is cleared; ticking every value
 * (other than "without this tag") turns the key into "any value".
 */
export function toggleValue(selection: TagSelection, group: TagKeyGroup, value: string | null): TagSelection {
  const next = new Map(selection);
  const current = selection.get(group.id);
  const values =
    current && current.size === 0
      ? new Set<string | null>(group.values.filter((v) => v !== null))
      : new Set<string | null>(current ?? []);
  if (values.has(value)) values.delete(value);
  else values.add(value);
  const present = group.values.filter((v): v is string => v !== null);
  if (values.size === 0) next.delete(group.id);
  else if (!values.has(null) && present.length > 0 && present.every((v) => values.has(v))) next.set(group.id, new Set());
  else next.set(group.id, values);
  return next;
}

/** The distinct (tag id, value) pairs across *items*, as filter options. */
export function tagOptionsOf(items: Iterable<ItemTags>): TagOption[] {
  const seen = new Set<string>();
  const out: TagOption[] = [];
  for (const tags of items) {
    for (const [id, values] of tags) {
      for (const value of values) {
        const token = `${id}=${value}`;
        if (seen.has(token)) continue;
        seen.add(token);
        out.push({ id, value });
      }
    }
  }
  return out;
}
