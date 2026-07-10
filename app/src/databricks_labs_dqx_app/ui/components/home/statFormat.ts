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
