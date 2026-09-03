# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ When Every Gauge Reads Normal and the Machine Still Fails
# MAGIC
# MAGIC ## Learn Row Anomaly Detection on Machine Telemetry in 10 Minutes
# MAGIC
# MAGIC **What you'll do:**
# MAGIC - Generate healthy telemetry where the metrics move together, as real machines do
# MAGIC - Break the *relationship* between three of them while every reading stays in range
# MAGIC - Prove no threshold or range check could ever catch it
# MAGIC - Train with `profile="timeseries"` and read the explanation
# MAGIC
# MAGIC **Dataset**: Eight metrics per reading across four CNC machines (no domain expertise required)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## The problem: a broken relationship is not an extreme value
# MAGIC
# MAGIC A plant records spindle load, motor current, coolant flow, bearing temperature, vibration,
# MAGIC hydraulic pressure, air pressure and throughput. Every metric has a safe band, and every band has
# MAGIC an alert.
# MAGIC
# MAGIC A bearing fails. Afterwards the logs show every gauge sat inside its band for two hours
# MAGIC beforehand. No alert fired.
# MAGIC
# MAGIC But something *was* visible: **motor current stopped tracking spindle load**. On a healthy machine
# MAGIC those rise and fall together, because cutting harder draws more current. For two hours load was
# MAGIC high while current sat mid-band. Both readings were ordinary. Their relationship was not.
# MAGIC
# MAGIC ## Two kinds of anomaly, two profiles
# MAGIC
# MAGIC | Your data | `profile` | An anomaly looks like |
# MAGIC |---|---|---|
# MAGIC | Independent records — transactions, orders, customers | `"tabular"` (default) | A row whose values, or combination of values, is unusual |
# MAGIC | Repeated multivariate measurements — machine or service metrics, sensors | `"timeseries"` | Metrics that normally move **together** stop doing so, each staying in its own range |
# MAGIC
# MAGIC Use `"timeseries"` for data like this. The default detector splits on one feature at a time, so a
# MAGIC broken relationship between two in-range values is close to invisible to it. On the **Server Machine
# MAGIC Dataset** — 28 machines of real telemetry with labelled incidents — the correlation-aware detector
# MAGIC surfaces **79%** of incidents inside an alert budget of 1% of rows, against **33%** for the default.
# MAGIC
# MAGIC **It needs no timestamp column.** It models correlation *between metrics*, not behaviour over time,
# MAGIC and never looks at row order. Include a timestamp anyway and DQX derives calendar features from it
# MAGIC (hour, day of week, month, weekend) exactly as it does for the tabular profile.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites: Install DQX with Anomaly Support
# MAGIC
# MAGIC ```python
# MAGIC %pip install 'databricks-labs-dqx[anomaly]'
# MAGIC ```
# MAGIC
# MAGIC **Note**: On ML Runtime or Serverless most dependencies are already present.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Install DQX

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install 'databricks-labs-dqx[anomaly]'

%restart_python

# COMMAND ----------
# DBTITLE 1,Configure catalog and schema

dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 1: Setup & Healthy Telemetry
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `machine_id` | string | `CNC-01` … `CNC-04` |
# MAGIC | `reading_seq` | int | Reading order — for reference only, the model never uses it |
# MAGIC | `spindle_load` … `throughput` | double | The eight metrics |
# MAGIC | `is_incident` | double | Ground truth, for this demo only — never given to the model |
# MAGIC
# MAGIC Healthy readings are driven by **two hidden factors**: how hard the machine is working, and how hot
# MAGIC it is running. Every metric is a mix of the two plus its own noise, which is exactly why they move
# MAGIC together — and what a correlation-aware detector learns.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Setup engines

import numpy as np
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.config import AnomalyParams, InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

ws = WorkspaceClient()
dq_engine = DQEngine(ws)
anomaly_engine = AnomalyEngine(ws)

print(f"✅ Setup complete — writing to {catalog}.{schema}")

# COMMAND ----------
# DBTITLE 1,Prepare a clean model registry

# Drop the registry so each run of this notebook starts from nothing. Without this, re-running leaves
# every previous run's rows behind and the "registered model" cell below shows a pile of stale
# configurations rather than the one just trained.
registry_table = f"{catalog}.{schema}.dqx_anomaly_models"
spark.sql(f"DROP TABLE IF EXISTS {registry_table}")

print(f"📋 Model registry: {registry_table}")
print("✅ Registry reset — ready for this run's model")

# COMMAND ----------
# DBTITLE 1,A helper for reading detection quality honestly


def report_quality(scored_df, label_col: str, severity_col, budget: float, thresholds=(90, 95, 98, 99)):
    """Print recall, precision and the best precision the budget allows, at several thresholds.

    Precision alone is unreadable here. Ask for the top 5% of 480 rows and you get 24 alerts; if only 24
    rows are genuinely wrong, no model can do better than 24/24, and if the budget yields 65 alerts the
    ceiling is 24/65 = 37% however good the ranking is. So the ceiling is printed beside what was
    achieved -- where the two are equal, the ranking is optimal and only the budget is costing you.
    """
    total = scored_df.count()
    faults = scored_df.filter(F.col(label_col) == 1.0).count()
    print(f"🎚️  {total:,} rows, {faults} of them genuinely wrong ({faults / total:.1%}).\n")
    print("Threshold | Alerts | Caught | Precision | Best possible | Recall")
    print("-" * 68)
    for threshold in thresholds:
        alerts = scored_df.filter(severity_col >= threshold)
        n_alerts = alerts.count()
        n_caught = alerts.filter(F.col(label_col) == 1.0).count()
        ceiling = min(n_alerts, faults) / n_alerts if n_alerts else 0.0
        precision = n_caught / n_alerts if n_alerts else 0.0
        recall = n_caught / faults if faults else 0.0
        marker = "  ← used above" if abs(threshold - budget) < 0.01 else ""
        print(
            f"{threshold:9} | {n_alerts:6} | {n_caught:3}/{faults:<3} | {precision:8.1%}  | "
            f"{ceiling:11.1%}   | {recall:6.1%}{marker}"
        )


print("✅ Helper ready")

# COMMAND ----------
# DBTITLE 1,How the metrics relate to each other

METRICS = [
    "spindle_load",
    "motor_current",
    "coolant_flow",
    "bearing_temp",
    "vibration",
    "hydraulic_pressure",
    "air_pressure",
    "throughput",
]

# Each row is one metric's sensitivity to (work rate, heat). Note motor_current tracks spindle_load
# closely, and air_pressure is mostly independent — not everything correlates, which is realistic.
LOADINGS = np.array(
    [[0.95, 0.10], [0.90, 0.15], [0.35, 0.80], [0.30, 0.90], [0.70, 0.45], [0.80, 0.20], [0.25, 0.30], [0.85, 0.25]]
).T
BASELINES = np.array([62.0, 18.5, 24.0, 58.0, 2.4, 145.0, 6.2, 480.0])
SCALES = np.array([9.0, 2.6, 3.4, 6.5, 0.45, 12.0, 0.35, 55.0])
SCHEMA = "machine_id string, reading_seq int, " + ", ".join(f"{m} double" for m in METRICS) + ", is_incident double"

print(f"📊 {len(METRICS)} metrics driven by 2 hidden factors")

# COMMAND ----------
# DBTITLE 1,Generate telemetry


def generate_telemetry(n_rows: int, seed: int, break_correlation: bool = False):
    """Telemetry driven by two shared latent factors.

    With *break_correlation*, a contiguous block has three metrics decoupled by **permuting their values
    among those rows** — the same values, reordered. Every metric's own distribution is preserved
    exactly, so only the joint behaviour changes.
    """
    rng = np.random.default_rng(seed)
    factors = rng.normal(0.0, 1.0, size=(n_rows, 2))
    values = BASELINES + SCALES * (factors @ LOADINGS + rng.normal(0.0, 0.18, size=(n_rows, len(METRICS))))
    labels = np.zeros(n_rows)

    if break_correlation:
        start, length = int(n_rows * 0.72), max(4, int(n_rows * 0.04))
        block = values[start : start + length, :3]
        values[start : start + length, :3] = np.roll(block, shift=1, axis=0)
        labels[start : start + length] = 1.0

    rows = [(f"CNC-{(i % 4) + 1:02d}", i, *[float(v) for v in values[i]], float(labels[i])) for i in range(n_rows)]
    return spark.createDataFrame(rows, SCHEMA)


# COMMAND ----------
# DBTITLE 1,Create the training table

# Nothing is cached in this notebook: PERSIST is unsupported on serverless compute, which is what most
# readers will run this on. These frames are small local relations built from seeded RNGs, so
# recomputation is both cheap and deterministic.
print("🔄 Generating healthy telemetry...\n")

healthy_df = generate_telemetry(4000, seed=5)
healthy_table = f"{catalog}.{schema}.fleet_telemetry_healthy"
healthy_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(healthy_table)

print("📊 Sample of healthy readings:")
display(healthy_df.limit(10))

print(f"\n✅ {healthy_df.count():,} readings saved to {healthy_table}")

# COMMAND ----------
# DBTITLE 1,Confirm the metrics really do move together

PAIRS = [
    ("spindle_load", "motor_current", "cutting harder draws more current"),
    ("bearing_temp", "coolant_flow", "coolant responds to heat"),
    ("spindle_load", "air_pressure", "barely related — and that is realistic"),
]

correlations = spark.table(healthy_table).select(
    *[F.round(F.corr(left, right), 3).alias(f"{left}__{right}") for left, right, _ in PAIRS]
).first()

print("🔍 Correlations on healthy data — the premise this detector relies on:\n")
print(f"{'metric pair':<34}{'correlation':>13}   why")
print("-" * 78)

for left, right, reason in PAIRS:
    print(f"{left + ' vs ' + right:<34}{correlations[f'{left}__{right}']:>13.3f}   {reason}")

print("\n💡 Strong pairs are what the detector exploits. Not every metric relates to every other,")
print("   and real telemetry looks exactly like this.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 2: The Incident
# MAGIC
# MAGIC Two hours in which spindle load, motor current and coolant flow stop tracking each other, while
# MAGIC every single reading stays inside the range it has always occupied.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Inject the correlation break

print("🔄 Generating a batch containing the incident...\n")

incident_df = generate_telemetry(1500, seed=77, break_correlation=True)
incident_table = f"{catalog}.{schema}.fleet_telemetry_incident"
incident_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(incident_table)

total_readings = incident_df.count()
injected = incident_df.filter(F.col("is_incident") == 1.0).count()

print(f"✅ {total_readings:,} readings saved to {incident_table}")
print(f"   {injected} of them during the incident")

# COMMAND ----------
# DBTITLE 1,Prove no range check could catch it

readings = spark.table(incident_table)
during = readings.filter(F.col("is_incident") == 1.0)
outside = readings.filter(F.col("is_incident") == 0.0)

print("🔍 Each metric's healthy range vs its range during the incident:\n")
print(f"{'metric':<18}{'healthy range':>22}{'during incident':>22}   verdict")
print("-" * 74)

for metric in METRICS[:3]:
    healthy = outside.select(F.min(metric).alias("lo"), F.max(metric).alias("hi")).first()
    broken = during.select(F.min(metric).alias("lo"), F.max(metric).alias("hi")).first()
    inside = broken["lo"] >= healthy["lo"] and broken["hi"] <= healthy["hi"]
    healthy_range = f"{healthy.lo:.1f} – {healthy.hi:.1f}"
    broken_range = f"{broken.lo:.1f} – {broken.hi:.1f}"
    print(f"{metric:<18}{healthy_range:>22}{broken_range:>22}   {'inside ✅' if inside else 'OUTSIDE'}")

print("\n⚠️  Every incident reading sits inside the healthy range for its own metric.")
print("   No threshold, no range check and no per-metric z-score can separate these rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 3: Train with `profile="timeseries"`
# MAGIC
# MAGIC One word selects the correlation-aware detector. Everything else is unchanged: the same automatic
# MAGIC feature engineering, the same registry, the same `has_no_row_anomalies` check, the same
# MAGIC contributions and AI explanations.
# MAGIC
# MAGIC It measures how far a reading sits from normal **once the relationships between metrics are
# MAGIC accounted for**. A load/current pair that never co-occurs on healthy data is far away in that
# MAGIC space even though each value sits mid-range.
# MAGIC
# MAGIC `baseline_by=[]` keeps the comparison across the whole fleet, because these four machines are
# MAGIC interchangeable and share one operating envelope. Pass `baseline_by=["machine_id"]` instead when
# MAGIC each machine has its own normal.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Train the model

print("🎯 Training the correlation-aware model...\n")

model_name = f"{catalog}.{schema}.fleet_telemetry_monitor"

trained = anomaly_engine.train(
    df=spark.table(healthy_table),
    model_name=model_name,
    registry_table=registry_table,
    columns=METRICS,
    baseline_by=[],
    profile="timeseries",
    # Whole table rather than the default sample: 4,000 readings is small enough that sampling only makes
    # the numbers printed below vary between runs, because the sample is drawn per partition.
    params=AnomalyParams(sample_fraction=1.0),
)

print(f"\n✅ Model trained: {trained}")

# COMMAND ----------
# DBTITLE 1,The registry records which detector was used

print("📋 Registered model:\n")

display(
    spark.table(registry_table)
    .filter(F.col("identity.model_name") == trained)
    .selectExpr(
        "identity.model_name",
        "identity.algorithm",
        "training.hyperparameters['covariance'] as covariance",
        "training.training_rows",
    )
)

print("💡 Scoring reads the algorithm back off the registry, so you never repeat the choice.")
print("   It also trains a single model rather than an ensemble, because it is deterministic.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 4: Score and Read the Explanation
# MAGIC
# MAGIC The detector explains itself by leaving each metric out in turn and reporting how much of the
# MAGIC anomaly disappears — so contributions work here with no SHAP involved.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Apply the anomaly check

print("🔍 Scoring the incident batch...\n")

anomaly_check = [
    DQDatasetRule(
        criticality="error",
        check_func=has_no_row_anomalies,
        check_func_kwargs={
            "model_name": trained,
            "registry_table": registry_table,
            "threshold": 95.0,
        },
    )
]

# One DQX call: name the input table, name the output table. Writing the result rather than keeping a
# lazy DataFrame also matters here — AI explanations call an LLM through ai_query *inside* the scoring
# plan, so each action on an unmaterialised result would call the model again.
scored_table = f"{catalog}.{schema}.fleet_scored"

dq_engine.apply_checks_and_save_in_table(
    input_config=InputConfig(location=incident_table),
    output_config=OutputConfig(location=scored_table, mode="overwrite", options={"overwriteSchema": "true"}),
    checks=anomaly_check,
)

scored = spark.table(scored_table)
anomaly = F.element_at(F.col("_dq_info"), 1).getField("anomaly")
flagged = scored.filter(anomaly.getField("is_anomaly"))

print(f"✅ Scoring complete — {flagged.count()} of {total_readings:,} readings flagged")

# COMMAND ----------
# DBTITLE 1,Which relationships broke

caught = flagged.filter(F.col("is_incident") == 1.0).count()
n_alerts = flagged.count()
print(f"🔝 Caught {caught} of the {injected} incident readings — none of which any range check could see.")
print(f"   That cost {n_alerts} alerts on {total_readings:,} readings, so {caught / n_alerts:.0%} of them were real.")
print("   The next cell sweeps the threshold, which is the honest way to read that number.\n")

display(
    flagged.select(
        "machine_id",
        "reading_seq",
        F.round("spindle_load", 1).alias("spindle_load"),
        F.round("motor_current", 1).alias("motor_current"),
        anomaly.getField("severity_percentile").alias("severity"),
        anomaly.getField("contributions").alias("contributions"),
    )
    .orderBy(F.desc("severity"))
    .limit(10)
)

# COMMAND ----------
# DBTITLE 1,What the alert budget actually buys

# `threshold=95` means "above the 95th percentile of *training* severity", not "95% likely to be a
# problem". On a batch whose readings are mostly stranger than anything training held, far more than 5% of
# it clears that line, which is why 95 flags well over the 75 rows a 5% budget implies.
report_quality(scored, "is_incident", anomaly.getField("severity_percentile"), budget=95.0)

print("\n💡 The default is not the right answer here. Moving to 98 keeps most of the recall and throws")
print("   a small fraction of the false alarms, because severity ranks the incident readings well above")
print("   the healthy ones — the default budget was simply set against the training distribution rather")
print("   than this batch. Severity is stored for every row, so this table costs nothing to produce and")
print("   is how you should pick the number on your own data.")

# COMMAND ----------
# DBTITLE 1,Why each group was flagged, in plain language

# One explanation per *pattern*, not per row: readings driven by the same broken relationship share a
# single ai_query call, so cost scales with how many distinct problems there are rather than with how many
# readings have them. Grouping the display the same way is the only way to see that.
print("🤖 AI explanations. One call per pattern, however many readings share it:\n")

display(
    flagged.groupBy(
        anomaly.getField("ai_explanation").getField("narrative").alias("narrative"),
        anomaly.getField("ai_explanation").getField("business_impact").alias("impact"),
        anomaly.getField("ai_explanation").getField("action").alias("action"),
    )
    .agg(F.count("*").alias("readings"), F.min("machine_id").alias("example_machine"))
    .filter(F.col("narrative").isNotNull())
    .orderBy(F.desc("readings"))
)

print("💡 Note the wording: broken *relationships*, not abnormal metrics. DQX tells the model which")
print("   detector produced the contributions, so the explanation describes the right kind of problem.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 5: (Optional) A Third Question — Is This Normal *For Now*?
# MAGIC
# MAGIC Everything above compared each reading against the fleet's normal. There is a third question, and a
# MAGIC maintenance engineer asks it constantly:
# MAGIC
# MAGIC > *Bearing temperature is 71°C. That is fine for this machine. Is it fine for **1,800 hours in**?*
# MAGIC
# MAGIC A wearing bearing has a **rising baseline**. A reading that is ordinary against the whole service
# MAGIC history can be well above where the wear curve had actually got to. `baseline_over_time` fits each
# MAGIC metric's expected level as a function of time and compares against *that*.
# MAGIC
# MAGIC | Question | Argument |
# MAGIC |---|---|
# MAGIC | Unusual on its own, or in combination? | `profile` |
# MAGIC | Unusual for its own group? | `baseline_by` |
# MAGIC | Unusual for its own point in time? | `baseline_over_time` |
# MAGIC
# MAGIC **This needs a different dataset, and that is the lesson.** The telemetry above is stationary by
# MAGIC construction — no trend, no timestamp — which is exactly the shape where a temporal baseline has
# MAGIC nothing to remove. So we generate a short service history that genuinely wears.

# COMMAND ----------
# DBTITLE 1,Generate a service history with a rising baseline

import datetime

WEAR_START = datetime.datetime(2025, 1, 6)


def generate_wear_history(n_hours: int, seed: int, late_fault: bool = False):
    """Bearing temperature and vibration that drift upward as the bearing wears.

    With *late_fault*, a block near the end is held at the level it had 600 hours earlier. Every value
    stays inside the range the whole history covers, so nothing about it is extreme -- it is simply wrong
    for how worn the bearing should be by then.
    """
    rng = np.random.default_rng(seed)
    hours = np.arange(n_hours)
    temp = 52.0 + 0.011 * hours + 4.0 * np.sin(2 * np.pi * hours / 24.0) + rng.normal(0, 1.1, n_hours)
    vib = 1.7 + 0.0006 * hours + rng.normal(0, 0.08, n_hours)
    labels = np.zeros(n_hours)

    if late_fault:
        start, length = int(n_hours * 0.80), max(6, int(n_hours * 0.05))
        temp[start : start + length] -= 0.011 * 600
        vib[start : start + length] -= 0.0006 * 600
        labels[start : start + length] = 1.0

    rows = [
        (WEAR_START + datetime.timedelta(hours=int(h)), float(temp[i]), float(vib[i]), float(labels[i]))
        for i, h in enumerate(hours)
    ]
    return spark.createDataFrame(rows, "reading_ts timestamp, bearing_temp double, vibration double, is_incident double")


wear_train = f"{catalog}.{schema}.bearing_wear_history"
wear_test = f"{catalog}.{schema}.bearing_wear_recent"
generate_wear_history(24 * 45, seed=11).write.mode("overwrite").saveAsTable(wear_train)
generate_wear_history(24 * 20, seed=12, late_fault=True).write.mode("overwrite").saveAsTable(wear_test)

print(f"📊 45 days of hourly service history, and 20 days of recent readings with a fault")

# COMMAND ----------
# DBTITLE 1,Measure whether the data trends before deciding

# The decision comes first, and it is measurable. Subtracting a fitted expectation from a metric with no
# temporal structure removes real signal and adds the fit's own error, so this is not a free switch.
from databricks.labs.dqx.anomaly.temporal_advisory import measure_trend_strength

WEAR_METRICS = ["bearing_temp", "vibration"]
wear_trend = measure_trend_strength(spark.table(wear_train), "reading_ts", WEAR_METRICS)
flat_trend = measure_trend_strength(
    spark.table(healthy_table).withColumn("fake_ts", F.expr("timestamp('2025-01-06') + make_interval(0,0,0,0,reading_seq)")),
    "fake_ts",
    METRICS,
)

print(f"📊 Wear history:        {wear_trend:.1%} of variance explained by trend and seasonality  ✅ use it")
print(f"📊 Fleet telemetry:     {flat_trend:.1%}  ⚠️  nothing to remove — DQX would warn, so leave it off")

# COMMAND ----------
# DBTITLE 1,Train with a time axis

# reading_ts is named as the axis, so it is NOT a feature: no cyclical calendar columns are derived from
# it. That matters — those help a calendar-contextual anomaly and measurably hurt otherwise.
wear_model = f"{catalog}.{schema}.bearing_wear_model"

anomaly_engine.train(
    df=spark.table(wear_train),
    model_name=wear_model,
    registry_table=registry_table,
    columns=WEAR_METRICS,
    baseline_over_time="reading_ts",
    baseline_by=[],
    params=AnomalyParams(sample_fraction=1.0),
)

print(f"🎯 Trained with an expected level per metric over time")

# COMMAND ----------
# DBTITLE 1,Score, and see what "wrong for now" looks like

# 99, not the 95 used earlier. Measured on this data, 95 raised 65 alerts for 24 real faults while 99
# raised exactly 24 and got all of them -- because a residual against a fitted expectation is a sharper
# signal than a raw value, so the severity distribution of a faulty batch sits far above training's. The
# next cell prints the sweep this was chosen from; do the same on your own data rather than copying 99.
WEAR_THRESHOLD = 99

wear_checks = [
    {
        "criticality": "error",
        "check": {
            "function": "has_no_row_anomalies",
            "arguments": {
                "model_name": wear_model,
                "registry_table": registry_table,
                "threshold": WEAR_THRESHOLD,
            },
        },
    }
]
wear_scored = f"{catalog}.{schema}.bearing_wear_scored"
dq_engine.apply_checks_by_metadata_and_save_in_table(
    input_config=InputConfig(location=wear_test),
    output_config=OutputConfig(location=wear_scored, mode="overwrite"),
    checks=wear_checks,
)

anomaly = F.col("_dq_info")[0].getField("anomaly")
wear_result = spark.table(wear_scored)
caught = wear_result.filter(anomaly.getField("is_anomaly") & (F.col("is_incident") == 1.0)).count()
total = wear_result.filter(F.col("is_incident") == 1.0).count()
print(f"🔍 Caught {caught} of {total} rows that were wrong for how worn the bearing should have been.\n")

# The ranking is what to judge, and a sweep is the only way to see it: every fault sits above every
# healthy row here, so a tighter budget costs no recall at all. That is unusual, and the reason to look.
report_quality(wear_result, "is_incident", anomaly.getField("severity_percentile"), budget=WEAR_THRESHOLD)

# COMMAND ----------
# DBTITLE 1,Read the contributions, which name the expected level

print("💡 Contributions read 'X vs its expected level at that time', not 'unusual X'.")
print("   The distinction is real: every one of these readings sits inside the history's own range.")
display(
    spark.table(wear_scored)
    .filter(anomaly.getField("is_anomaly"))
    .select(
        "reading_ts",
        "bearing_temp",
        anomaly.getField("severity_percentile").alias("severity"),
        anomaly.getField("contributions").alias("contributions"),
        anomaly.getField("is_stale_baseline").alias("extrapolating"),
    )
    .orderBy(F.desc("severity"))
    .limit(8)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### When *not* to reach for `baseline_over_time`
# MAGIC
# MAGIC The cell that measured both datasets is the point of this section. The honest cases against it:
# MAGIC
# MAGIC - **A largely stationary metric**, like the fleet telemetry above. On real server telemetry that
# MAGIC   arrives already normalised, the same transform measured *worse* than leaving it off.
# MAGIC - **A short training window.** A daily shape needs several complete days to be identifiable at all.
# MAGIC   DQX fits one only where the window supports it, and logs the period it skipped and why.
# MAGIC - **With `profile="tabular"`, keep other datetime columns out of `columns`.** Calendar features on
# MAGIC   top of the residual measured worse on every anomaly shape tested.
# MAGIC
# MAGIC It is also **not a forecaster**. It models the level expected *at* a time; it does not predict the
# MAGIC next value, and it never reads the previous row — which is what keeps scoring valid on a stream.
# MAGIC
# MAGIC `is_stale_baseline` marks rows past the window the expectation was fitted on. The score is still
# MAGIC produced, because near the boundary it is still accurate; treat the flag as a signal to retrain.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Summary & Next Steps
# MAGIC
# MAGIC **Key takeaways:**
# MAGIC - Some failures are **broken relationships**, not extreme values. Every gauge reads normal and the
# MAGIC   machine is still in trouble.
# MAGIC - Per-metric alerts cannot see those, however well tuned — verified above, not asserted.
# MAGIC - `profile="timeseries"` switches to a detector that models how metrics move together. One word;
# MAGIC   everything else is unchanged.
# MAGIC - It needs no timestamp column and trains a single model rather than an ensemble.
# MAGIC
# MAGIC **Apply to your data:**
# MAGIC ```python
# MAGIC model = anomaly_engine.train(
# MAGIC     df=spark.table("your_catalog.your_schema.your_metrics"),
# MAGIC     model_name="your_catalog.your_schema.your_model",
# MAGIC     registry_table="your_catalog.your_schema.dqx_anomaly_models",
# MAGIC     profile="timeseries",
# MAGIC )
# MAGIC
# MAGIC # Then score straight from one table into another.
# MAGIC dq_engine.apply_checks_and_save_in_table(
# MAGIC     input_config=InputConfig(location="your_catalog.your_schema.new_readings"),
# MAGIC     output_config=OutputConfig(location="your_catalog.your_schema.scored"),
# MAGIC     checks=checks,
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **What this does not do**, so you can plan around it:
# MAGIC - **Trend.** A steadily growing metric eventually leaves the range it was trained on. Model a rate
# MAGIC   or a ratio rather than a running level, and retrain on a schedule.
# MAGIC - **A single metric.** With nothing to correlate against, use a rule or a threshold.
# MAGIC - **Forecasting.** DQX judges readings against learned normal; it does not predict the next value.
# MAGIC
# MAGIC The SMD figures quoted earlier are a large gain on the data this detector is for, and they are *not*
# MAGIC state of the art for multivariate time-series anomaly detection. Published figures near 0.80 F1 use
# MAGIC point adjustment, which [Kim et al. (AAAI 2022)](https://arxiv.org/abs/2109.05257) showed random
# MAGIC scores also reach, so they are not a fair comparison in either direction.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Resources
# MAGIC
# MAGIC - [Choosing a profile](https://databrickslabs.github.io/dqx/docs/guide/row_anomaly_detection#choosing-a-profile)
# MAGIC - [Benchmarks](https://databrickslabs.github.io/dqx/docs/reference/benchmarks#anomaly-benchmarks) — measured detection quality and timings
# MAGIC - [Row Anomaly Detection guide](https://databrickslabs.github.io/dqx/docs/guide/row_anomaly_detection)
# MAGIC
# MAGIC ### 🎉 You're Ready!
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ The difference between an extreme value and a broken relationship
# MAGIC - ✅ When to reach for `profile="timeseries"` instead of the default
# MAGIC - ✅ Why it needs no timestamp column
# MAGIC - ✅ How to read contributions and AI explanations for a correlation break
# MAGIC
# MAGIC **Start watching the relationships, not just the gauges!** 🚀
# MAGIC
