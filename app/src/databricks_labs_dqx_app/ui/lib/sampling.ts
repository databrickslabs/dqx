import type { SampleKind } from "@/components/rules/test/RuleTestPanel";

/** Default percentage used when a row count cannot carry over to the percent unit. */
export const DEFAULT_SAMPLE_PERCENT = 10;

/**
 * Value to carry over when the sampling unit changes.
 *
 * Switching records -> percent cannot reuse the number: a row count such as
 * 50000 is not a percentage, and *clamping* it lands on 100 — which is the whole
 * table, the opposite of the cap the user was expressing. Out-of-range values
 * therefore reset to {@link DEFAULT_SAMPLE_PERCENT} rather than saturating.
 *
 * Switching the other way (percent -> records) keeps the number: a small row
 * count is unusual but valid, so there is nothing to correct.
 */
export function sampleValueForKind(kind: SampleKind, current: number): number {
  if (kind !== "percent") return current;
  return current >= 1 && current <= 100 ? current : DEFAULT_SAMPLE_PERCENT;
}
