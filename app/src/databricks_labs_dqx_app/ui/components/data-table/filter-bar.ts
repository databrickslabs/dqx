/**
 * Shared sizing tokens for the overview filter/actions bars on the Rules
 * Registry, Monitored Tables, and Table Spaces overview pages, so the three
 * bars stay visually consistent (P25 item 29).
 *
 * `FILTER_TRIGGER_CLASS` is applied to every trigger-style filter control —
 * plain `Select` triggers, the searchable-combobox triggers, the label filter
 * and the Group by select — so the pills share one compact width and a full
 * filter bar wraps cleanly beside the Edit View button instead of pushing
 * it out of line. Option lists open wider than the trigger when needed. The
 * search box keeps its own wider `w-56` and is intentionally not tokenized
 * here.
 */
export const FILTER_TRIGGER_CLASS = "h-8 w-36 text-xs";

/**
 * DQ-score bucket filter shared by the Monitored Tables and Table Spaces
 * overviews (P25 item 91). The percentage boundaries match dqlake's
 * `BindingsTable` (0–25 / 25–50 / 50–75 / 75–100); the extra `"none"` bucket
 * selects rows with no cached score. `DQ_SCORE_FILTER_ALL` is the "no filter"
 * sentinel. Buckets are ordered high→low for the dropdown, matching dqlake.
 */
export const DQ_SCORE_FILTER_ALL = "all";

export const DQ_SCORE_BUCKETS: readonly { value: string; labelKey: string }[] = [
  { value: DQ_SCORE_FILTER_ALL, labelKey: "common.dqScoreFilter.any" },
  { value: "75-100", labelKey: "common.dqScoreFilter.b75_100" },
  { value: "50-75", labelKey: "common.dqScoreFilter.b50_75" },
  { value: "25-50", labelKey: "common.dqScoreFilter.b25_50" },
  { value: "0-25", labelKey: "common.dqScoreFilter.b0_25" },
  { value: "none", labelKey: "common.dqScoreFilter.none" },
];

/**
 * True when a row whose cached DQ score is *score* (in [0, 1], or null/undefined
 * when there is no cached score yet) passes the selected *bucket*. A null score
 * matches only the `"none"` bucket; a present score never matches `"none"`.
 */
export function matchesDqScoreBucket(score: number | null | undefined, bucket: string): boolean {
  if (bucket === DQ_SCORE_FILTER_ALL) return true;
  if (score == null) return bucket === "none";
  if (bucket === "none") return false;
  const pct = score * 100;
  switch (bucket) {
    case "0-25":
      return pct < 25;
    case "25-50":
      return pct >= 25 && pct < 50;
    case "50-75":
      return pct >= 50 && pct < 75;
    case "75-100":
      return pct >= 75;
    default:
      return true;
  }
}

/**
 * The DQ-score bucket a row falls in — the same bands as
 * {@link matchesDqScoreBucket}, so grouping by score and filtering by score
 * always agree. A null/undefined score is `"none"`.
 */
export function dqScoreBucketOf(score: number | null | undefined): string {
  const bucket = DQ_SCORE_BUCKETS.find(
    (b) => b.value !== DQ_SCORE_FILTER_ALL && matchesDqScoreBucket(score, b.value),
  );
  return bucket?.value ?? "none";
}
