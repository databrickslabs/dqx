/**
 * The states an object's History can compare: its approved versions, each
 * saved draft (a create or edit step that recorded the object's state), and,
 * in View changes, the proposed (not yet approved) changes. Also what picking
 * an entry of the History timeline compares.
 */

/** One side of a comparison: an approved version (0 = nothing), a history step's state, or the proposed changes. */
export type SideRef = { kind: "version"; version: number } | { kind: "step"; historyId: number } | { kind: "proposed" };

export const NOTHING: SideRef = { kind: "version", version: 0 };
export const PROPOSED_REF: SideRef = { kind: "proposed" };

export function versionRef(version: number): SideRef {
  return { kind: "version", version };
}

export function stepRef(historyId: number): SideRef {
  return { kind: "step", historyId };
}

/** A stable string for a side (picker values, React keys). */
export function refKey(ref: SideRef): string {
  if (ref.kind === "version") return `v${ref.version}`;
  if (ref.kind === "step") return `s${ref.historyId}`;
  return "proposed";
}

export function parseRefKey(key: string): SideRef {
  if (key === "proposed") return PROPOSED_REF;
  const id = Number(key.slice(1));
  return key.startsWith("s") ? stepRef(id) : versionRef(id);
}

export function sameRef(a: SideRef, b: SideRef): boolean {
  return refKey(a) === refKey(b);
}

/** A history step as the timeline knows it. */
export interface TimelineStep {
  action: string;
  at?: string | null;
  historyId?: number | null;
  hasSnapshot?: boolean;
}

/** An approved version as the timeline knows it. */
export interface TimelineVersion {
  version: number;
  createdAt?: string | null;
}

/** A comparable state, newest first in :func:`historyPoints`. */
export interface HistoryPoint {
  ref: SideRef;
  /** Epoch ms; -Infinity when unknown. */
  at: number;
  /** When it was saved (an ISO string), for a draft's label. */
  atIso?: string | null;
  /**
   * The state the object was created with. Not offered as a draft to compare,
   * nor as the state before the first edit (that compares with nothing); only
   * what a submit reviewed when nothing was edited in between.
   */
  created?: boolean;
}

/** Steps that leave a saved draft: the state they recorded is one the user saved. */
const DRAFT_ACTIONS = new Set(["created", "edited"]);

function time(at: string | null | undefined): number {
  const ms = at ? Date.parse(at) : Number.NaN;
  return Number.isNaN(ms) ? Number.NEGATIVE_INFINITY : ms;
}

/** The approved versions and saved drafts (and the created state, see :type:`HistoryPoint`), newest first. */
export function historyPoints(versions: TimelineVersion[], steps: TimelineStep[]): HistoryPoint[] {
  const points: HistoryPoint[] = [
    ...versions.map((v) => ({ ref: versionRef(v.version), at: time(v.createdAt), atIso: v.createdAt })),
    ...steps
      .filter((s) => s.hasSnapshot && s.historyId != null && DRAFT_ACTIONS.has(s.action))
      .map((s) => ({
        ref: stepRef(s.historyId as number),
        at: time(s.at),
        atIso: s.at,
        created: s.action === "created",
      })),
  ];
  // Same (or unknown) time: the later-recorded first — a step by its id, a version by its number.
  const order = (p: HistoryPoint) => (p.ref.kind === "step" ? p.ref.historyId : p.ref.kind === "version" ? p.ref.version : 0);
  return points.sort((a, b) => b.at - a.at || order(b) - order(a));
}

/** The time of a side, for ordering (the proposed changes are the newest; nothing is the oldest). */
export function pointTime(ref: SideRef, points: HistoryPoint[]): number {
  if (ref.kind === "proposed") return Number.POSITIVE_INFINITY;
  if (ref.kind === "version" && ref.version === 0) return Number.NEGATIVE_INFINITY;
  return points.find((p) => sameRef(p.ref, ref))?.at ?? Number.NEGATIVE_INFINITY;
}

/** The states offered in the Compare / To pickers: everything but the created state. */
export function pickerPoints(points: HistoryPoint[]): HistoryPoint[] {
  return points.filter((p) => !p.created);
}

/**
 * What picking a timeline entry compares, or null when it has nothing to show:
 * always nothing against the object at that point in time (pick two states in
 * the Compare / To pickers to compare them with each other).
 *
 * - A version: the version.
 * - Creating the object: nothing (it's where the history starts).
 * - A saved draft (an edit): the draft.
 * - A submit, rejection or withdrawal: the draft under review then.
 * - A step that didn't record its state: the version its review cycle produced.
 */
export function selectionFor(
  entry: { kind: "version"; version: number } | ({ kind: "step" } & TimelineStep),
  points: HistoryPoint[],
): { base: SideRef; target: SideRef } | null {
  const fromNothing = (target: SideRef) => ({ base: NOTHING, target });
  if (entry.kind === "version") return fromNothing(versionRef(entry.version));
  if (entry.action === "created") return fromNothing(NOTHING);
  const at = time(entry.at);
  if (entry.hasSnapshot && entry.historyId != null && DRAFT_ACTIONS.has(entry.action)) {
    return fromNothing(stepRef(entry.historyId));
  }
  if (entry.action === "submitted" || entry.action === "rejected" || entry.action === "withdrawn") {
    const reviewed = points.find((p) => p.at <= at);
    return reviewed ? fromNothing(reviewed.ref) : null;
  }
  // No recorded state: the version its review cycle produced (the first one after it).
  const produced = [...points].reverse().find((p) => p.ref.kind === "version" && p.at > at);
  return produced ? fromNothing(produced.ref) : null;
}
