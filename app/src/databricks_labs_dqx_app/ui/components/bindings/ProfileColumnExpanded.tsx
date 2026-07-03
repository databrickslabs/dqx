import { useTranslation } from "react-i18next";
import { fmtStat, typeGroup, type StatTypeGroup } from "@/lib/profile-format";

interface Props {
  stats: Record<string, unknown>;
  rowCount: number | null | undefined;
  sparkType: string | undefined;
}

/**
 * DQX's profiler summary_json keys (see DQProfiler._profile / _get_df_summary_as_dict):
 *   count, count_null, count_non_null, count_distinct, empty_count,
 *   mean, stddev, min, max, "25%", "50%", "75%".
 * "Most common values" has no DQX equivalent (the profiler doesn't compute
 * top-N value frequencies), so that section always renders the
 * "unavailable" fallback — the layout still matches the dqlake design.
 */

const PERCENTILE_KEYS = new Set(["25%", "50%", "75%"]);

/** Keys rendered as their own indicator elsewhere in the collapsed row — don't repeat. */
const HIDDEN_KEYS = new Set([
  "count_non_null", // completeness bar
  "count_distinct", // distinct bar
  "count", // implied by rows profiled
]);

function orderKeys(group: StatTypeGroup, present: string[]): string[] {
  const base: string[] =
    group === "numeric"
      ? ["count_null", "empty_count", "min", "max", "mean", "stddev"]
      : group === "string"
        ? ["count_null", "empty_count", "min_length", "max_length", "avg_length"]
        : group === "timestamp"
          ? ["count_null", "empty_count", "min", "max"]
          : ["count_null", "empty_count"];
  const ordered = base.filter((k) => present.includes(k));
  const extras = present.filter((k) => !base.includes(k) && !PERCENTILE_KEYS.has(k)).sort();
  return [...ordered, ...extras];
}

export function ProfileColumnExpanded({ stats, rowCount, sparkType }: Props) {
  const { t } = useTranslation();
  const group = typeGroup(sparkType);
  const presentKeys = Object.keys(stats).filter((k) => !HIDDEN_KEYS.has(k) && stats[k] != null);
  const ordered = orderKeys(group, presentKeys);

  const LABELS: Record<string, string> = {
    count_null: t("monitoredTables.profileStatCountNull"),
    count_non_null: t("monitoredTables.profileStatCountNonNull"),
    count_distinct: t("monitoredTables.profileStatCountDistinct"),
    empty_count: t("monitoredTables.profileStatEmptyCount"),
    min: t("monitoredTables.profileStatMin"),
    max: t("monitoredTables.profileStatMax"),
    mean: t("monitoredTables.profileStatMean"),
    stddev: t("monitoredTables.profileStatStddev"),
    "25%": t("monitoredTables.profileStatP25"),
    "50%": t("monitoredTables.profileStatMedian"),
    "75%": t("monitoredTables.profileStatP75"),
    max_length: t("monitoredTables.profileStatMaxLength"),
    min_length: t("monitoredTables.profileStatMinLength"),
    avg_length: t("monitoredTables.profileStatAvgLength"),
  };

  const showPercentiles =
    group === "numeric" && Array.from(PERCENTILE_KEYS).some((k) => stats[k] != null);

  void rowCount; // reserved for a future "Most common values" implementation

  return (
    <div className="grid grid-cols-1 sm:grid-cols-[1.4fr_1fr] gap-6 px-4 pt-3 pb-4 pl-12">
      <div>
        <h5 className="text-[10px] uppercase tracking-wide text-muted-foreground font-medium mb-2">
          {t("monitoredTables.profileStatisticsHeading")}
        </h5>
        <div className="grid grid-cols-4 gap-x-5 gap-y-2">
          {ordered.map((key) => (
            <div key={key}>
              <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
                {LABELS[key] ?? key}
              </div>
              <div className="font-mono text-xs break-all">{fmtStat(stats[key])}</div>
            </div>
          ))}
          {showPercentiles && (
            <div>
              <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
                {t("monitoredTables.profileStatPercentiles")}
              </div>
              <div className="font-mono text-xs">
                {fmtStat(stats["25%"])} / {fmtStat(stats["50%"])} / {fmtStat(stats["75%"])}
              </div>
            </div>
          )}
        </div>
      </div>
      <div>
        <h5 className="text-[10px] uppercase tracking-wide text-muted-foreground font-medium mb-2">
          {t("monitoredTables.profileMostCommonValuesHeading")}
        </h5>
        <p className="text-[11px] text-muted-foreground italic">
          {t("monitoredTables.profileTopValuesUnavailable")}
        </p>
      </div>
    </div>
  );
}
