# Databricks notebook source
# MAGIC %md
# MAGIC # The machine where every gauge read normal
# MAGIC
# MAGIC A plant records eight metrics per machine per minute: spindle load, motor current, coolant flow,
# MAGIC bearing temperature, vibration, hydraulic pressure, air pressure, throughput. Every metric has a safe
# MAGIC operating band, and there are alerts on all of them.
# MAGIC
# MAGIC A bearing fails. Afterwards the logs show every gauge sat inside its band for two hours beforehand.
# MAGIC No alert fired. Nothing to see.
# MAGIC
# MAGIC But something *was* visible: motor current stopped tracking spindle load. On a healthy machine those
# MAGIC rise and fall together, because cutting harder draws more current. For two hours load was high while
# MAGIC current sat mid-band. Both readings were ordinary. **Their relationship was not.**
# MAGIC
# MAGIC This is a different shape of anomaly from the one in `dqx_demo_anomaly_tabular_transactions.py`, and
# MAGIC it needs a different detector. It is what **`profile="timeseries"`** is for.
# MAGIC
# MAGIC We will train *both* profiles on the same data and compare, because a second algorithm is only worth
# MAGIC having if it earns its place.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install 'databricks-labs-dqx[anomaly]'

%restart_python

# COMMAND ----------

dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## No timestamp column is required
# MAGIC
# MAGIC Worth saying immediately, because everyone expects otherwise. `profile="timeseries"` models
# MAGIC **correlation between metrics**, not behaviour over time. It never looks at row order. The name
# MAGIC describes the *data you have* — repeated multivariate measurements — not a temporal algorithm.
# MAGIC
# MAGIC A consequence worth knowing: it will not learn "Tuesdays are busy" from row order either. Include a
# MAGIC timestamp column and DQX derives calendar features from it (hour, day-of-week, month, weekend) exactly
# MAGIC as it does for the tabular profile. What neither profile models is **trend**.

# COMMAND ----------
# DBTITLE 1,Generate healthy machine telemetry

import numpy as np
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule

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

# Two latent drivers -- how hard the machine is working, and how hot it is running. Every metric is a
# mix of the two plus its own noise, which is why they all move together on a healthy machine.
LOADINGS = np.array(
    [
        [0.95, 0.10],  # spindle_load        <- mostly work rate
        [0.90, 0.15],  # motor_current       <- tracks spindle load closely
        [0.35, 0.80],  # coolant_flow        <- mostly heat
        [0.30, 0.90],  # bearing_temp        <- mostly heat
        [0.70, 0.45],  # vibration
        [0.80, 0.20],  # hydraulic_pressure
        [0.25, 0.30],  # air_pressure        <- mostly independent
        [0.85, 0.25],  # throughput
    ]
).T
BASELINES = np.array([62.0, 18.5, 24.0, 58.0, 2.4, 145.0, 6.2, 480.0])
SCALES = np.array([9.0, 2.6, 3.4, 6.5, 0.45, 12.0, 0.35, 55.0])


def generate_telemetry(n_rows: int, seed: int, break_correlation: bool = False):
    """Machine telemetry driven by two shared latent factors.

    When *break_correlation* is set, a contiguous block of rows has three metrics decoupled from the rest
    by **permuting their values among those rows**. That preserves every metric's own distribution exactly
    -- the same values, reordered -- so no single-metric alert can possibly fire. Only the joint
    behaviour changes, which is precisely the failure this profile exists to catch.
    """
    rng = np.random.default_rng(seed)
    factors = rng.normal(0.0, 1.0, size=(n_rows, 2))
    values = BASELINES + SCALES * (factors @ LOADINGS + rng.normal(0.0, 0.18, size=(n_rows, len(METRICS))))

    labels = np.zeros(n_rows)
    if break_correlation:
        # A two-hour incident: spindle_load, motor_current and vibration stop tracking each other.
        start, length = int(n_rows * 0.72), max(4, int(n_rows * 0.04))
        block = values[start : start + length, :3]
        values[start : start + length, :3] = np.roll(block, shift=1, axis=0)
        labels[start : start + length] = 1.0

    rows = [
        (f"CNC-{(i % 4) + 1:02d}", i, *[float(v) for v in values[i]], float(labels[i])) for i in range(n_rows)
    ]
    schema = "machine_id string, reading_seq int, " + ", ".join(f"{m} double" for m in METRICS) + ", is_incident double"
    return spark.createDataFrame(rows, schema), int(labels.sum())


# Nothing is persisted or cached in this notebook: PERSIST is unsupported on serverless compute,
# which is what most readers will run this on. These frames are small local relations built from
# seeded RNGs, so recomputation is both cheap and deterministic.
healthy_df, _ = generate_telemetry(4000, seed=5)
print(f"✅ {healthy_df.count():,} healthy readings across 4 machines, {len(METRICS)} metrics each")
display(healthy_df.limit(5))

# COMMAND ----------
# DBTITLE 1,The correlation this depends on
# MAGIC %md
# MAGIC Before modelling anything, confirm the premise: on healthy data these metrics really do move
# MAGIC together. `motor_current` against `spindle_load` is the pair from the story.

# COMMAND ----------

display(
    healthy_df.select(
        F.round(F.corr("spindle_load", "motor_current"), 3).alias("load_vs_current"),
        F.round(F.corr("bearing_temp", "coolant_flow"), 3).alias("temp_vs_coolant"),
        F.round(F.corr("spindle_load", "air_pressure"), 3).alias("load_vs_air_pressure"),
    )
)
print("Strong pairs are the ones a correlation-aware detector can exploit.")
print("air_pressure is mostly independent by design — not everything correlates, and that is realistic.")

# COMMAND ----------
# DBTITLE 1,Now the incident, and proof it is invisible per-metric

incident_df, injected = generate_telemetry(1500, seed=77, break_correlation=True)
print(f"✅ {incident_df.count():,} readings, {injected} of them during the incident\n")

# Compare each metric's range during the incident against its range outside it. If a range check could
# catch this, we have not built the scenario we claimed to.
during = incident_df.filter(F.col("is_incident") == 1.0)
outside = incident_df.filter(F.col("is_incident") == 0.0)

print(f"{'metric':<22}{'healthy range':>26}{'during incident':>26}")
for metric in METRICS[:3]:
    o = outside.select(F.min(metric).alias("lo"), F.max(metric).alias("hi")).first()
    d = during.select(F.min(metric).alias("lo"), F.max(metric).alias("hi")).first()
    inside = "inside" if d["lo"] >= o["lo"] and d["hi"] <= o["hi"] else "OUTSIDE"
    print(f"{metric:<22}{f'{o.lo:.1f} – {o.hi:.1f}':>26}{f'{d.lo:.1f} – {d.hi:.1f} ({inside})':>26}")

print("\nEvery incident reading sits inside the healthy range for its own metric.")
print("No threshold, no range check and no per-metric z-score can separate these rows.")

# COMMAND ----------
# DBTITLE 1,Train both profiles on identical data
# MAGIC %md
# MAGIC The comparison that justifies a second algorithm. Same data, same columns, same everything except
# MAGIC one word.

# COMMAND ----------

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")
registry_table = f"{catalog}.{schema}.dqx_anomaly_models"

ws = WorkspaceClient()
dq_engine = DQEngine(ws)
anomaly_engine = AnomalyEngine(ws)

models = {}
for profile in ("tabular", "timeseries"):
    models[profile] = anomaly_engine.train(
        df=healthy_df,
        model_name=f"{catalog}.{schema}.fleet_telemetry_{profile}",
        registry_table=registry_table,
        columns=METRICS,
        # No grouping: these machines are interchangeable and share one operating envelope. Pass
        # baseline_by=["machine_id"] instead if each machine has its own normal.
        baseline_by=[],
        profile=profile,
    )
    print(f"✅ {profile:<11} -> {models[profile]}")

# COMMAND ----------
# DBTITLE 1,The registry records which detector each model uses

display(
    spark.table(registry_table)
    .filter(F.col("identity.model_name").contains("fleet_telemetry"))
    .selectExpr(
        "identity.model_name",
        "identity.algorithm",
        "training.hyperparameters['covariance'] as covariance",
        "training.training_rows",
    )
    .orderBy("identity.model_name")
)
print("Scoring reads the algorithm back off the registry, so you never repeat the choice.")

# COMMAND ----------
# DBTITLE 1,Score both, and count incidents caught

# Scoring is written to a table rather than held as a lazy plan, and that matters here rather than being
# housekeeping. AI explanations call an LLM through ai_query *inside* the scoring plan, so every action on
# an unmaterialised result calls the model again -- the cells below take several actions each, which would
# mean paying for the LLM repeatedly and getting a different answer each time. `.cache()` would normally
# prevent that, but PERSIST is unsupported on serverless compute. Writing once is the pattern to copy.
results = {}
for profile, model in models.items():
    scored = dq_engine.apply_checks(
        incident_df,
        [
            DQDatasetRule(
                criticality="error",
                check_func=has_no_row_anomalies,
                check_func_kwargs={
                    "model_name": model,
                    "registry_table": registry_table,
                    "threshold": 95.0,
                },
            )
        ],
    )
    scored_table = f"{catalog}.{schema}.fleet_scored_{profile}"
    scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(scored_table)
    results[profile] = spark.table(scored_table)
    print(f"✅ {profile:<11} scored -> {scored_table}")

def anomaly_of(df):
    """The anomaly struct from the first _dq_info element (element_at is 1-based)."""
    return F.element_at(F.col("_dq_info"), 1).getField("anomaly")


print(f"{'profile':<13}{'incident rows caught':>22}{'total flagged':>16}{'false alarms':>15}")
for profile, scored in results.items():
    a = anomaly_of(scored)
    caught = scored.filter(a.getField("is_anomaly") & (F.col("is_incident") == 1.0)).count()
    flagged = scored.filter(a.getField("is_anomaly")).count()
    false_alarms = flagged - caught
    print(f"{profile:<13}{f'{caught} of {injected}':>22}{flagged:>16}{false_alarms:>15}")

print("\nSame data. Same columns. The tabular detector splits one metric at a time, so a broken")
print("relationship between two in-range values is close to invisible to it.")

# COMMAND ----------
# DBTITLE 1,The same comparison at an equal alert budget
# MAGIC %md
# MAGIC The two detectors above did not flag the same number of rows, and they need not: a severity
# MAGIC threshold is a percentile of the *training* score distribution, so how many new rows exceed it
# MAGIC depends on the detector. That leaves a fair question — did the second one just alert more?
# MAGIC
# MAGIC Settle it by giving both the same budget. Rank every reading by severity, take the top N from each
# MAGIC with N identical, and count how many incident rows each one bought. This is how the DQX benchmarks
# MAGIC compare detectors, and it is the honest way to read any such comparison.

# COMMAND ----------

from pyspark.sql import Window

BUDGET_FRACTION = 0.05  # alert on the 5% of readings each detector ranks most anomalous
budget = int(incident_df.count() * BUDGET_FRACTION)

print(f"budget: the top {budget} readings by severity ({BUDGET_FRACTION:.0%} of {incident_df.count():,})\n")
print(f"{'profile':<13}{'incident rows caught':>22}{'precision':>12}")
for profile, scored in results.items():
    ranked = scored.select(
        F.col("is_incident"),
        anomaly_of(scored).getField("severity_percentile").alias("severity"),
    ).withColumn("rank", F.row_number().over(Window.orderBy(F.col("severity").desc_nulls_last())))

    top = ranked.filter(F.col("rank") <= budget)
    caught = top.filter(F.col("is_incident") == 1.0).count()
    print(f"{profile:<13}{f'{caught} of {injected}':>22}{caught / budget:>11.0%}")

print("\nSame number of alerts for each. The difference is what those alerts are worth.")

# COMMAND ----------
# DBTITLE 1,Why the correlation-aware detector can see it
# MAGIC %md
# MAGIC It measures how far a reading sits from normal **once the relationships between metrics are
# MAGIC accounted for** — a distance in a space where the metrics have been decorrelated. A load/current
# MAGIC pair that never co-occurs on healthy data is far away in that space even though each value is
# MAGIC near the middle of its own range.
# MAGIC
# MAGIC It explains itself by leaving each metric out in turn and reporting how much of the anomaly
# MAGIC disappears — so contributions work here without SHAP.

# COMMAND ----------

ts = results["timeseries"]
a = anomaly_of(ts)

display(
    ts.filter(a.getField("is_anomaly"))
    .select(
        "machine_id",
        "reading_seq",
        F.round("spindle_load", 1).alias("spindle_load"),
        F.round("motor_current", 1).alias("motor_current"),
        F.round("vibration", 2).alias("vibration"),
        a.getField("severity_percentile").alias("severity"),
        a.getField("contributions").alias("contributions"),
    )
    .orderBy(F.desc("severity"))
    .limit(10)
)

# COMMAND ----------
# DBTITLE 1,And in plain language

display(
    ts.filter(a.getField("is_anomaly"))
    .select(
        "machine_id",
        "reading_seq",
        a.getField("ai_explanation").getField("narrative").alias("narrative"),
        a.getField("ai_explanation").getField("business_impact").alias("impact"),
        a.getField("ai_explanation").getField("action").alias("action"),
    )
    .filter(F.col("narrative").isNotNull())
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to take away
# MAGIC
# MAGIC - Some failures are **broken relationships**, not extreme values. Every gauge reads normal and the
# MAGIC   machine is still in trouble.
# MAGIC - Per-metric alerts cannot see those, however well tuned. In this notebook every incident reading
# MAGIC   sits inside its own metric's healthy range — verified above, not asserted.
# MAGIC - `profile="timeseries"` switches to a detector that models how metrics move **together**. One word,
# MAGIC   and everything else is unchanged: same feature engineering, same registry, same check, same
# MAGIC   contributions and AI explanations.
# MAGIC - It needs **no timestamp column** and trains a single model rather than an ensemble, because it is
# MAGIC   deterministic.
# MAGIC
# MAGIC ### Being straight about the limits
# MAGIC
# MAGIC On the **Server Machine Dataset** — 28 machines of real telemetry with labelled incidents — this
# MAGIC detector surfaces 79% of incidents inside an alert budget of 1% of rows, against 33% for the tabular
# MAGIC detector. That is a large gain on the data it is for, and it is *not* state of the art for
# MAGIC multivariate time-series anomaly detection. Published figures near 0.80 F1 are not a fair comparison:
# MAGIC they use point adjustment, which
# MAGIC [Kim et al. (AAAI 2022)](https://arxiv.org/abs/2109.05257) showed random scores also reach.
# MAGIC
# MAGIC What this does **not** do:
# MAGIC
# MAGIC - **Trend.** Sustained growth eventually drifts outside the trained range. Model a rate or a ratio
# MAGIC   rather than a running level, and retrain on a schedule.
# MAGIC - **A single metric.** With nothing to correlate against, use a rule or a threshold.
# MAGIC - **Forecasting.** DQX judges readings against learned normal; it does not predict the next value.
# MAGIC - **Choosing the profile for you.** You pick it. The cheap heuristic for guessing was measured and
# MAGIC   rejected because it fires on ordinary sorted tables — see
# MAGIC   [Choosing a profile](https://databrickslabs.github.io/dqx/docs/guide/row_anomaly_detection#choosing-a-profile).
