/**
 * Shared sizing tokens for the overview filter/actions bars on the Rules
 * Registry, Monitored Tables, and Table Spaces overview pages, so the three
 * bars stay visually consistent (P25 item 29).
 *
 * `FILTER_TRIGGER_CLASS` is applied to every trigger-style filter control —
 * plain `Select` triggers, the searchable-combobox triggers, and the free-text
 * tag input — so they share one width. It is deliberately a touch wider than
 * the old per-page values (RR was `w-36`, MT/TS were `w-40`) so longer option
 * labels (statuses, stewards, score buckets) don't crowd the chevron. The
 * search box keeps its own wider `w-56` and is intentionally not tokenized
 * here.
 */
export const FILTER_TRIGGER_CLASS = "h-8 w-44 text-xs";
