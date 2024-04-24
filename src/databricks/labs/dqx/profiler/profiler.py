import datetime
import math
from dataclasses import dataclass
from typing import Any, Optional

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


@dataclass
class DQRule:
    name: str
    column: str
    description: Optional[str] = None
    parameters: Optional[dict[str, Any]] = None


def do_cast(vl: Optional[str], typ: T.DataType) -> Optional[Any]:
    if not vl:
        return None
    if typ == T.IntegerType() or typ == T.LongType():
        return int(vl)
    if typ == T.DoubleType() or typ == T.FloatType():
        return float(vl)

    # TODO: handle other types

    return vl


def get_df_summary_as_dict(df: DataFrame) -> dict[str, Any]:
    """Generate summary for Dataframe & return it as dictionary with column name as a key, and dict of metric/value

    :param df: dataframe to profile
    :return: dict with metrics per column
    """
    sm_dict: dict[str, dict] = {}
    field_types = dict([(f.name, f.dataType) for f in df.schema.fields])
    for row in df.summary().collect():
        d = row.asDict()
        metric = d["summary"]
        for nm, vl in d.items():
            if nm == "summary":
                continue
            if nm not in sm_dict:
                sm_dict[nm] = {}
            typ = field_types[nm]
            if (typ == T.IntegerType() or typ == T.LongType()) and (metric == "stddev" or metric == "mean"):
                sm_dict[nm][metric] = float(vl)
            else:
                sm_dict[nm][metric] = do_cast(vl, typ)

    return sm_dict


def type_supports_distinct(typ: T.DataType) -> bool:
    return typ == T.StringType() or typ == T.IntegerType() or typ == T.LongType()


# TODO: add decimal & integral types
def type_supports_min_max(typ: T.DataType) -> bool:
    return (
        typ == T.IntegerType()
        or typ == T.LongType()
        or typ == T.FloatType()
        or typ == T.DateType()
        or typ == T.TimestampType()
    )


def round_value(v: Any, direction: str, opts: dict[str, Any]) -> Any:
    if not v or not opts.get("round", False):
        return v

    if isinstance(v, datetime.datetime):
        if direction == "down":
            return v.replace(hour=0, minute=0, second=0, microsecond=0)
        elif direction == "up":
            return v.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

    if isinstance(v, float):
        if direction == "down":
            return math.floor(v)
        elif direction == "up":
            return math.ceil(v)

    # TODO: add rounding for integers, etc.?

    return v


default_profile_options = {
    "round": True,
    "max_in_count": 10,
    "distinct_ratio": 0.05,
    "max_null_ratio": 0.01,  # Generate is_null if we have less than 1 percent of nulls
    "remove_outliers": True,
    # detect outliers for generation of range conditions. should it be configurable per column?
    "outlier_columns": [],  # remove outliers in all columns of appropriate type
    "num_sigmas": 3,  # number of sigmas to use when remove_outliers is True
    "trim_strings": True,  # trim whitespace from strings
    "max_empty_ratio": 0.01,
}


def extract_min_max(dst: DataFrame, nm: str, typ, metrics, opts: dict[str, Any] = {}) -> Optional[DQRule]:
    """Generates a rule for ranges.

    :param dst: Single-column DataFrame
    :param nm: name of the column
    :param typ: type of the column
    :param metrics: holder for metrics
    :param opts: options
    :return:
    """
    descr = None
    mn = None
    mx = None

    outlier_cols = opts.get("outlier_columns", [])
    cl = dst.columns[0]
    if opts.get("remove_outliers", True) and (len(outlier_cols) == 0 or nm in outlier_cols):  # detect outliers
        if typ == T.DateType():
            dst = dst.select(F.col(cl).cast("timestamp").cast("bigint").alias(cl))
        elif typ == T.TimestampType():
            dst = dst.select(F.col(cl).cast("bigint").alias(cl))
        # TODO: do summary instead? to get percentiles, etc.?
        mn_mx = dst.agg(F.min(cl), F.max(cl), F.mean(cl), F.stddev(cl)).collect()
        if mn_mx and len(mn_mx) > 0:
            metrics["min"] = mn_mx[0][0]
            metrics["max"] = mn_mx[0][1]
            sigmas = opts.get("sigmas", 3)
            avg = mn_mx[0][2]
            stddev = mn_mx[0][3]
            mn = avg - sigmas * stddev
            mx = avg + sigmas * stddev
            if mn > mn_mx[0][0] and mx < mn_mx[0][1]:
                descr = (
                    f"Range doesn't include outliers, capped by {sigmas} sigmas. avg={avg}, "
                    f"stddev={stddev}, min={metrics.get('min')}, max={metrics.get('max')}"
                )
            elif mn < mn_mx[0][0] and mx > mn_mx[0][1]:  #
                mn = mn_mx[0][0]
                mx = mn_mx[0][1]
                descr = "Real min/max values were used"
            elif mn < mn_mx[0][0]:
                mn = mn_mx[0][0]
                descr = (
                    f"Real min value was used. Max was capped by {sigmas} sigmas. avg={avg}, "
                    f"stddev={stddev}, max={metrics.get('max')}"
                )
            elif mx > mn_mx[0][1]:
                mx = mn_mx[0][1]
                descr = (
                    f"Real max value was used. Min was capped by {sigmas} sigmas. avg={avg}, "
                    f"stddev={stddev}, min={metrics.get('min')}"
                )
            # we need to preserve type at the end
            if typ == T.IntegerType() or typ == T.LongType():
                mn = int(round_value(mn, "down", {"round": True}))
                mx = int(round_value(mx, "up", {"round": True}))
            elif typ == T.DateType():
                mn = datetime.date.fromtimestamp(int(mn))
                mx = datetime.date.fromtimestamp(int(mx))
                metrics["min"] = datetime.date.fromtimestamp(int(metrics["min"]))
                metrics["max"] = datetime.date.fromtimestamp(int(metrics["max"]))
                metrics["mean"] = datetime.date.fromtimestamp(int(avg))
            elif typ == T.TimestampType():
                mn = round_value(datetime.datetime.fromtimestamp(int(mn)), "down", {"round": True})
                mx = round_value(datetime.datetime.fromtimestamp(int(mx)), "up", {"round": True})
                metrics["min"] = datetime.datetime.fromtimestamp(int(metrics["min"]))
                metrics["max"] = datetime.datetime.fromtimestamp(int(metrics["max"]))
                metrics["mean"] = datetime.datetime.fromtimestamp(int(avg))
        else:
            print(f"Can't get min/max for field {nm}")
    else:
        mn_mx = dst.agg(F.min(cl), F.max(cl)).collect()
        if mn_mx and len(mn_mx) > 0:
            metrics["min"] = mn_mx[0][0]
            metrics["max"] = mn_mx[0][1]
            mn = round_value(metrics.get("min"), "down", opts)
            mx = round_value(metrics.get("max"), "up", opts)
            descr = "Real min/max values were used"
        else:
            print(f"Can't get min/max for field {nm}")
    if descr and mn and mx:
        return DQRule(name="min_max", column=nm, parameters={"min": mn, "max": mx}, description=descr)

    return None


def get_fields(nm: str, c: T.StructType) -> list[T.StructField]:
    fields = []
    for f in c.fields:
        if isinstance(f.dataType, T.StructType):
            fields.extend(get_fields(f.name, f.dataType))
        else:
            fields.append(f)

    return [T.StructField(f"{nm}.{f.name}", f.dataType, f.nullable) for f in fields]


def get_columns_or_fields(cols: list[T.StructField]) -> list[T.StructField]:
    t = []
    for c in cols:
        nm = c.name
        if isinstance(c.dataType, T.StructType):
            t.extend(get_fields(nm, c.dataType))
        else:
            t.append(c)

    return t


# TODO: split into managebale chunks
# TODO: how to handle maps, arrays & structs?
# TODO: return not only DQ rules, but also the profiling results - use named tuple?
def profile_dataframe(
    df: DataFrame, cols: Optional[list[str]] = None, opts: dict[str, Any] = {}
) -> tuple[dict[str, Any], list[DQRule]]:
    if opts is None:
        opts = {}
    dq_rules: list[DQRule] = []

    if not cols:
        cols = df.columns
    df_cols = [f for f in df.schema.fields if f.name in cols]
    df = df.select(*[f.name for f in df_cols])
    df.cache()
    total_count = df.count()
    summary_stats = get_df_summary_as_dict(df)
    if total_count == 0:
        return summary_stats, dq_rules

    opts = {**default_profile_options, **opts}
    max_nulls = opts.get("max_null_ratio", 0)
    trim_strings = opts.get("trim_strings", True)

    # TODO: think, how we can do it in fewer passes. Maybe only for specific things, like, min_max, etc.
    for field in get_columns_or_fields(df_cols):
        nm = field.name
        typ = field.dataType
        if nm not in summary_stats:
            summary_stats[nm] = {}
        metrics = summary_stats[nm]

        # calculate metrics
        dst = df.select(nm).dropna()
        if typ == T.StringType() and trim_strings:
            cl = dst.columns[0]
            dst = dst.select(F.trim(F.col(cl)).alias(cl))

        dst.cache()
        metrics["count"] = total_count
        count_non_null = dst.count()
        metrics["count_non_null"] = count_non_null
        metrics["count_null"] = total_count - count_non_null

        if count_non_null >= (total_count * (1 - max_nulls)):
            if count_non_null != total_count:
                null_percentage = 1 - (1.0 * count_non_null) / total_count
                dq_rules.append(
                    DQRule(
                        name="is_not_null",
                        column=nm,
                        description=f"Column {nm} has {null_percentage * 100:.1f}% of null values "
                        f"(allowed {max_nulls * 100:.1f}%)",
                    )
                )
            else:
                dq_rules.append(DQRule(name="is_not_null", column=nm))

        if type_supports_distinct(typ):
            dst2 = dst.dropDuplicates()
            cnt = dst2.count()
            if 0 < cnt < total_count * opts["distinct_ratio"] and cnt < opts["max_in_count"]:
                dq_rules.append(DQRule(name="is_in", column=nm, parameters={"in": [row[0] for row in dst2.collect()]}))

        if typ == T.StringType():
            dst2 = dst.filter(F.col(nm) == "")
            cnt = dst2.count()
            if cnt <= (metrics["count"] * opts.get("max_empty_ratio", 0)):
                dq_rules.append(
                    DQRule(name="is_not_null_or_empty", column=nm, parameters={"trim_strings": trim_strings})
                )

        if metrics["count_non_null"] > 0 and type_supports_min_max(typ):
            rule = extract_min_max(dst, nm, typ, metrics, opts)
            if rule:
                dq_rules.append(rule)

        # That should be the last one
        dst.unpersist()

    return summary_stats, dq_rules
