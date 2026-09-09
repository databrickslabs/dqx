/**
 * Pure stat-shaping helpers for the homepage "At a Glance" cards
 * (dqlake's HomeStats count-up/formatting logic, extracted so it is
 * unit-testable without a DOM).
 */

/** Animated count-up value at progress `t` (0..1, clamped) toward `target`,
 *  using easeOutCubic — the same curve dqlake's `useCountUp` ticks with. */
export function countUpValue(target: number, t: number): number {
  const clamped = Math.min(1, Math.max(0, t));
  if (clamped >= 1) return target;
  return target * (1 - Math.pow(1 - clamped, 3));
}

/** Whole-number, locale-grouped rendering of an animating count. */
export function formatCount(animated: number): string {
  return Math.round(animated).toLocaleString();
}

/**
 * Score-card value: an em dash while the score cache has never produced a
 * global score (`score` null/undefined), otherwise the animated percentage
 * to one decimal. `score` is the raw 0..1 fraction; `animatedPercent` is
 * the count-up value already scaled to 0..100.
 */
export function formatScorePercent(
  score: number | null | undefined,
  animatedPercent: number,
): string {
  return score == null ? "—" : `${animatedPercent.toFixed(1)}%`;
}

export type DeltaDirection = "up" | "down" | "flat";

/**
 * Direction of the score change since the previous run — dqlake's
 * DeltaIndicator thresholds. `delta` is a 0..1 fraction (e.g. +0.05 =
 * +5 percentage points); moves under 0.05pp read as flat because the
 * score itself is shown to one decimal.
 */
export function deltaDirection(delta: number): DeltaDirection {
  const pp = delta * 100;
  if (pp >= 0.05) return "up";
  if (pp <= -0.05) return "down";
  return "flat";
}

/** Magnitude of the change in percentage points, one decimal (for the
 *  delta badge tooltip: "Up 5.0 points since the previous run"). */
export function deltaPoints(delta: number): string {
  return Math.abs(delta * 100).toFixed(1);
}
