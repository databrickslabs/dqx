import type { SideRef } from "./history-points";
import { itemStatus } from "./yaml-diff";

function targetIsNothing(target: SideRef): boolean {
  return target.kind === "version" && target.version === 0;
}

/** Only a user-picked comparison needs the explicit request; defaults already have the same data. */
export function shouldQueryExplicitChanges(
  enabled: boolean,
  defaultsReady: boolean,
  isDefault: boolean,
  target: SideRef,
): boolean {
  return enabled && defaultsReady && !isDefault && !targetIsNothing(target);
}

/** The response currently shown, with a Nothing target represented by no response. */
export function comparisonToShow<T>(
  defaults: T | undefined,
  explicit: T | undefined,
  isDefault: boolean,
  target: SideRef,
): T | undefined {
  if (targetIsNothing(target)) return undefined;
  return isDefault ? defaults : explicit;
}

/** Which legacy-version explanation applies to the comparison currently shown. */
export function unrecordedCollectionNoteKind(
  comparison:
    | {
        base_recorded: boolean;
        target_version?: number | null;
        target_step?: number | null;
      }
    | undefined,
): "pending" | "history" | null {
  if (!comparison || comparison.base_recorded) return null;
  return comparison.target_version == null && comparison.target_step == null ? "pending" : "history";
}

/** Pending status, or undefined while the approved versions needed to decide it are loading. */
export function rulePendingStatus(
  approved: number,
  versionsLoading: boolean,
  before: unknown[],
  after: unknown[],
): boolean | undefined {
  if (versionsLoading) return undefined;
  return approved === 0 || itemStatus(before, after) !== "unchanged";
}
