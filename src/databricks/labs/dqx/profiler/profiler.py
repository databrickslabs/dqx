import datetime
import math
from dataclasses import dataclass
from typing import Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


@dataclass
class DQProfile:
    name: str
    column: str
    description: str | None = None
    parameters: dict[str, Any] | None = None


def do_cast(value: str | None, typ: T.DataType) -> Any | None:
    if not value:
        return None
    if typ == T.IntegerType() or typ == T.LongType():
        return int(value)
    if typ == T.DoubleType() or typ == T.FloatType():
        return float(value)

    # TODO: handle other types

    return value


def get_df_summary_as_dict(df: DataFrame) -> dict[str, Any]:
    """Generate summary for Dataframe & return it as dictionary with column name as a key, and dict of metric/value

    :param df: dataframe to _profile
    :return: dict with metrics per column
    """
    sm_dict: dict[str, dict] = {}
    field_types = {f.name: f.dataType for f in df.schema.fields}
    for row in df.summary().collect():
        row_dict = row.asDict()
        metric = row_dict["summary"]
        for metric_name, metric_value in row_dict.items():
            if metric_name == "summary":
                continue
            if metric_name not in sm_dict:
                sm_dict[metric_name] = {}

            typ = field_types[metric_name]
            if (typ in {T.IntegerType(), T.LongType()}) and metric in {"stddev", "mean"}:
                sm_dict[metric_name][metric] = float(metric_value)
            else:
                sm_dict[metric_name][metric] = do_cast(metric_value, typ)

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


def round_value(value: Any, direction: str, opts: dict[str, Any]) -> Any:
    if not value or not opts.get("round", False):
        return value

    if isinstance(value, datetime.datetime):
        if direction == "down":
            return value.replace(hour=0, minute=0, second=0, microsecond=0)
        if direction == "up":
            return value.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

    if isinstance(value, float):
        if direction == "down":
            return math.floor(value)
        if direction == "up":
            return math.ceil(value)

    # TODO: add rounding for integers, etc.?

    return value


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


def extract_min_max(
    dst: DataFrame,
    col_name: str,
    typ: T.DataType,
    metrics: dict[str, Any],
    opts: dict[str, Any] | None = None,
) -> DQProfile | None:
    """Generates a rule for ranges.

    :param dst: Single-column DataFrame
    :param col_name: name of the column
    :param typ: type of the column
    :param metrics: holder for metrics
    :param opts: options
    :return:
    """
    descr = None
    min_limit = None
    max_limit = None

    if opts is None:
        opts = {}

    outlier_cols = opts.get("outlier_columns", [])
    column = dst.columns[0]
    if opts.get("remove_outliers", True) and (len(outlier_cols) == 0 or col_name in outlier_cols):  # detect outliers
        if typ == T.DateType():
            dst = dst.select(F.col(column).cast("timestamp").cast("bigint").alias(column))
        elif typ == T.TimestampType():
            dst = dst.select(F.col(column).cast("bigint").alias(column))
        # TODO: do summary instead? to get percentiles, etc.?
        mn_mx = dst.agg(F.min(column), F.max(column), F.mean(column), F.stddev(column)).collect()
        descr, max_limit, min_limit = get_min_max(col_name, descr, max_limit, metrics, min_limit, mn_mx, opts, typ)
    else:
        mn_mx = dst.agg(F.min(column), F.max(column)).collect()
        if mn_mx and len(mn_mx) > 0:
            metrics["min"] = mn_mx[0][0]
            metrics["max"] = mn_mx[0][1]
            min_limit = round_value(metrics.get("min"), "down", opts)
            max_limit = round_value(metrics.get("max"), "up", opts)
            descr = "Real min/max values were used"
        else:
            print(f"Can't get min/max for field {col_name}")
    if descr and min_limit and max_limit:
        return DQProfile(
            name="min_max", column=col_name, parameters={"min": min_limit, "max": max_limit}, description=descr
        )

    return None


def get_min_max(col_name, descr, max_limit, metrics, min_limit, mn_mx, opts, typ):
    if mn_mx and len(mn_mx) > 0:
        metrics["min"] = mn_mx[0][0]
        metrics["max"] = mn_mx[0][1]
        sigmas = opts.get("sigmas", 3)
        avg = mn_mx[0][2]
        stddev = mn_mx[0][3]
        min_limit = avg - sigmas * stddev
        max_limit = avg + sigmas * stddev
        if min_limit > mn_mx[0][0] and max_limit < mn_mx[0][1]:
            descr = (
                f"Range doesn't include outliers, capped by {sigmas} sigmas. avg={avg}, "
                f"stddev={stddev}, min={metrics.get('min')}, max={metrics.get('max')}"
            )
        elif min_limit < mn_mx[0][0] and max_limit > mn_mx[0][1]:  #
            min_limit = mn_mx[0][0]
            max_limit = mn_mx[0][1]
            descr = "Real min/max values were used"
        elif min_limit < mn_mx[0][0]:
            min_limit = mn_mx[0][0]
            descr = (
                f"Real min value was used. Max was capped by {sigmas} sigmas. avg={avg}, "
                f"stddev={stddev}, max={metrics.get('max')}"
            )
        elif max_limit > mn_mx[0][1]:
            max_limit = mn_mx[0][1]
            descr = (
                f"Real max value was used. Min was capped by {sigmas} sigmas. avg={avg}, "
                f"stddev={stddev}, min={metrics.get('min')}"
            )
        # we need to preserve type at the end
        if typ == T.IntegerType() or typ == T.LongType():
            min_limit = int(round_value(min_limit, "down", {"round": True}))
            max_limit = int(round_value(max_limit, "up", {"round": True}))
        elif typ == T.DateType():
            min_limit = datetime.date.fromtimestamp(int(min_limit))
            max_limit = datetime.date.fromtimestamp(int(max_limit))
            metrics["min"] = datetime.date.fromtimestamp(int(metrics["min"]))
            metrics["max"] = datetime.date.fromtimestamp(int(metrics["max"]))
            metrics["mean"] = datetime.date.fromtimestamp(int(avg))
        elif typ == T.TimestampType():
            min_limit = round_value(datetime.datetime.fromtimestamp(int(min_limit)), "down", {"round": True})
            max_limit = round_value(datetime.datetime.fromtimestamp(int(max_limit)), "up", {"round": True})
            metrics["min"] = datetime.datetime.fromtimestamp(int(metrics["min"]))
            metrics["max"] = datetime.datetime.fromtimestamp(int(metrics["max"]))
            metrics["mean"] = datetime.datetime.fromtimestamp(int(avg))
    else:
        print(f"Can't get min/max for field {col_name}")
    return descr, max_limit, min_limit


def get_fields(col_name: str, schema: T.StructType) -> list[T.StructField]:
    fields = []
    for f in schema.fields:
        if isinstance(f.dataType, T.StructType):
            fields.extend(get_fields(f.name, f.dataType))
        else:
            fields.append(f)

    return [T.StructField(f"{col_name}.{f.name}", f.dataType, f.nullable) for f in fields]


def get_columns_or_fields(cols: list[T.StructField]) -> list[T.StructField]:
    out_cols = []
    for column in cols:
        col_name = column.name
        if isinstance(column.dataType, T.StructType):
            out_cols.extend(get_fields(col_name, column.dataType))
        else:
            out_cols.append(column)

    return out_cols


# TODO: split into managebale chunks
# TODO: how to handle maps, arrays & structs?
# TODO: return not only DQ rules, but also the profiling results - use named tuple?
def profile(
    df: DataFrame, cols: list[str] | None = None, opts: dict[str, Any] | None = None
) -> tuple[dict[str, Any], list[DQProfile]]:
    if opts is None:
        opts = {}
    dq_rules: list[DQProfile] = []

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

    _profile(df, df_cols, dq_rules, max_nulls, opts, summary_stats, total_count, trim_strings)

    return summary_stats, dq_rules


def _profile(df, df_cols, dq_rules, max_nulls, opts, summary_stats, total_count, trim_strings):
    # TODO: think, how we can do it in fewer passes. Maybe only for specific things, like, min_max, etc.
    for field in get_columns_or_fields(df_cols):
        field_name = field.name
        typ = field.dataType
        if field_name not in summary_stats:
            summary_stats[field_name] = {}
        metrics = summary_stats[field_name]

        calculate_metrics(df, dq_rules, field_name, max_nulls, metrics, opts, total_count, trim_strings, typ)


def calculate_metrics(df, dq_rules, field_name, max_nulls, metrics, opts, total_count, trim_strings, typ):
    dst = df.select(field_name).dropna()
    if typ == T.StringType() and trim_strings:
        col_name = dst.columns[0]
        dst = dst.select(F.trim(F.col(col_name)).alias(col_name))
    dst.cache()
    metrics["count"] = total_count
    count_non_null = dst.count()
    metrics["count_non_null"] = count_non_null
    metrics["count_null"] = total_count - count_non_null
    if count_non_null >= (total_count * (1 - max_nulls)):
        if count_non_null != total_count:
            null_percentage = 1 - (1.0 * count_non_null) / total_count
            dq_rules.append(
                DQProfile(
                    name="is_not_null",
                    column=field_name,
                    description=f"Column {field_name} has {null_percentage * 100:.1f}% of null values "
                    f"(allowed {max_nulls * 100:.1f}%)",
                )
            )
        else:
            dq_rules.append(DQProfile(name="is_not_null", column=field_name))
    if type_supports_distinct(typ):
        dst2 = dst.dropDuplicates()
        cnt = dst2.count()
        if 0 < cnt < total_count * opts["distinct_ratio"] and cnt < opts["max_in_count"]:
            dq_rules.append(
                DQProfile(name="is_in", column=field_name, parameters={"in": [row[0] for row in dst2.collect()]})
            )
    if typ == T.StringType():
        dst2 = dst.filter(F.col(field_name) == "")
        cnt = dst2.count()
        if cnt <= (metrics["count"] * opts.get("max_empty_ratio", 0)):
            dq_rules.append(
                DQProfile(name="is_not_null_or_empty", column=field_name, parameters={"trim_strings": trim_strings})
            )
    if metrics["count_non_null"] > 0 and type_supports_min_max(typ):
        rule = extract_min_max(dst, field_name, typ, metrics, opts)
        if rule:
            dq_rules.append(rule)
    # That should be the last one
    dst.unpersist()
