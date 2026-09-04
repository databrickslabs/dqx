/** Total number of aiBuildExample* keys defined in the locales. */
export const AI_EXAMPLE_COUNT = 20;

/**
 * Pick a 1-based `aiBuildExampleN` i18n key at random. `rand` is injectable for
 * tests; defaults to Math.random. Clamps so the result is always in [1, count].
 */
export function pickAiExampleKey(count: number, rand: () => number = Math.random): string {
  const n = Math.min(count, Math.max(1, Math.floor(rand() * count) + 1));
  return `aiBuildExample${n}`;
}
