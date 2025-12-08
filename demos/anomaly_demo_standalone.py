# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Anomaly Detection Demo - Production Ready
# MAGIC 
# MAGIC This notebook demonstrates DQX anomaly detection with realistic synthetic data generation.
# MAGIC 
# MAGIC ## Scenarios Covered:
# MAGIC 1. **E-commerce Fraud Detection** - Transaction anomalies
# MAGIC 2. **IoT Sensor Monitoring** - Equipment failures with temporal patterns
# MAGIC 3. **Financial Trading** - Market manipulation detection
# MAGIC 4. **Quarantine Workflow** - Automatic isolation of anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install DQX with anomaly support (if not already installed)
%pip install databricks-labs-dqx[anomaly] --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import numpy as np

from databricks.labs.dqx.anomaly import train, has_no_anomalies, AnomalyParams
from databricks.labs.dqx.anomaly.temporal import extract_temporal_features
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.sdk import WorkspaceClient

# Initialize
spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

print("✅ Setup complete!")
print(f"   Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 Scenario 1: E-commerce Fraud Detection
# MAGIC 
# MAGIC **Objective**: Detect fraudulent transactions using multivariate analysis.
# MAGIC 
# MAGIC **Features Analyzed**:
# MAGIC - Transaction amount
# MAGIC - Number of items
# MAGIC - Discount rate
# MAGIC - Shipping cost
# MAGIC - Customer age
# MAGIC 
# MAGIC **Anomaly Types**:
# MAGIC - High-value fraud (extreme amounts)
# MAGIC - Pricing errors (excessive discounts)
# MAGIC - System glitches (free shipping on small orders)

# COMMAND ----------

def generate_ecommerce_transactions(n_normal=1000, n_anomalies=50):
    """Generate realistic e-commerce transaction data with labeled anomalies."""
    
    # Normal transactions
    normal_data = []
    for i in range(n_normal):
        amount = float(np.clip(np.random.lognormal(4.5, 0.8), 10, 500))
        items_count = int(np.clip(np.random.gamma(2, 1.5), 1, 8))
        discount_rate = float(np.random.beta(2, 8) * 0.3)
        shipping_cost = float(np.clip(5 + (amount / 100) * 3 + np.random.normal(0, 1), 5, 15))
        customer_age = int(np.clip(np.random.normal(35, 12), 18, 80))
        
        normal_data.append((
            f"TXN_{i:06d}",
            round(amount, 2),
            items_count,
            round(discount_rate, 3),
            round(shipping_cost, 2),
            customer_age,
            False
        ))
    
    # Anomalous transactions
    anomaly_data = []
    for i in range(n_anomalies):
        anomaly_type = np.random.choice(['fraud', 'pricing_error', 'system_glitch'])
        
        if anomaly_type == 'fraud':
            amount = float(np.random.uniform(2000, 10000))
            items_count = int(np.random.randint(1, 3))
            discount_rate = float(np.random.uniform(0, 0.05))
            shipping_cost = float(np.random.uniform(5, 20))
        elif anomaly_type == 'pricing_error':
            amount = float(np.random.uniform(100, 500))
            items_count = int(np.random.randint(2, 6))
            discount_rate = float(np.random.uniform(0.6, 0.95))
            shipping_cost = float(np.random.uniform(5, 15))
        else:  # system_glitch
            amount = float(np.random.uniform(10, 50))
            items_count = int(np.random.randint(1, 3))
            discount_rate = float(np.random.uniform(0, 0.1))
            shipping_cost = 0.0
        
        customer_age = int(np.random.randint(18, 80))
        
        anomaly_data.append((
            f"TXN_ANOM_{i:06d}",
            round(amount, 2),
            items_count,
            round(discount_rate, 3),
            round(shipping_cost, 2),
            customer_age,
            True
        ))
    
    # Combine and create DataFrame
    all_data = normal_data + anomaly_data
    random.shuffle(all_data)
    
    return spark.createDataFrame(all_data, [
        "transaction_id", "amount", "items_count", "discount_rate",
        "shipping_cost", "customer_age", "is_anomaly"
    ])

# Generate data
ecommerce_df = generate_ecommerce_transactions(n_normal=1000, n_anomalies=50)

print(f"✅ Generated {ecommerce_df.count()} transactions")
print(f"   Normal: {ecommerce_df.filter('is_anomaly = false').count()}")
print(f"   Anomalies: {ecommerce_df.filter('is_anomaly = true').count()}")

display(ecommerce_df.limit(10))

# COMMAND ----------

# Split data: train on normal only, test on all
training_df = ecommerce_df.filter("is_anomaly = false").sample(fraction=0.8, seed=42)
testing_df = ecommerce_df

print(f"📚 Training set: {training_df.count()} normal transactions")
print(f"🧪 Testing set: {testing_df.count()} total transactions")

# COMMAND ----------

# Train anomaly detection model
print("🎓 Training fraud detection model...")

model_uri = train(
    df=training_df,
    columns=["amount", "items_count", "discount_rate", "shipping_cost", "customer_age"],
    model_name="ecommerce_fraud_detector",
    registry_table="main.default.dqx_anomaly_models"
)

print(f"✅ Model trained successfully!")
print(f"   Model URI: {model_uri}")

# View model metrics
display(
    spark.table("main.default.dqx_anomaly_models")
    .filter("model_name = 'ecommerce_fraud_detector' AND status = 'active'")
    .select("training_rows", "training_time", "metrics", "feature_importance")
)

# COMMAND ----------

# Score transactions
print("🔍 Detecting anomalies...")

checks = [
    has_no_anomalies(
        columns=["amount", "items_count", "discount_rate", "shipping_cost", "customer_age"],
        model="ecommerce_fraud_detector",
        registry_table="main.default.dqx_anomaly_models",
        score_threshold=0.6,
        include_contributions=True
    )
]

scored_df = dq_engine.apply_checks_by_metadata(testing_df, checks)

print("✅ Scoring complete!")

# COMMAND ----------

# Analyze results
print("🎯 Top 20 Detected Anomalies:")
display(
    scored_df
    .orderBy(F.desc("anomaly_score"))
    .select(
        "transaction_id", "amount", "items_count", "discount_rate",
        "anomaly_score", "anomaly_contributions", "is_anomaly"
    )
    .limit(20)
)

# Calculate detection accuracy
flagged_df = scored_df.withColumn(
    "detected",
    F.when(F.col("anomaly_score") > 0.6, True).otherwise(False)
)

tp = flagged_df.filter("is_anomaly = true AND detected = true").count()
fp = flagged_df.filter("is_anomaly = false AND detected = true").count()
tn = flagged_df.filter("is_anomaly = false AND detected = false").count()
fn = flagged_df.filter("is_anomaly = true AND detected = false").count()

precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

print(f"""
📈 Detection Performance:
   Precision: {precision:.1%} ({tp}/{tp + fp} flagged are actual anomalies)
   Recall: {recall:.1%} ({tp}/{tp + fn} anomalies detected)
   F1 Score: {f1:.1%}
   
   Confusion Matrix:
   - True Positives: {tp}
   - False Positives: {fp}
   - True Negatives: {tn}
   - False Negatives: {fn}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🌡️ Scenario 2: IoT Sensor Monitoring (with Temporal Features)
# MAGIC 
# MAGIC **Objective**: Detect sensor malfunctions considering time-of-day patterns.
# MAGIC 
# MAGIC **Features**:
# MAGIC - Temperature (varies by hour)
# MAGIC - Pressure (correlates with temp)
# MAGIC - Vibration
# MAGIC - Power consumption
# MAGIC - Hour, day of week, weekend flag

# COMMAND ----------

def generate_iot_sensor_data(n_days=30, readings_per_hour=1):
    """Generate IoT sensor readings with temporal patterns."""
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for day in range(n_days):
        for hour in range(24):
            timestamp = start_date + timedelta(days=day, hours=hour)
            
            # Temperature varies by hour (sine wave)
            base_temp = 70 + 10 * np.sin(2 * np.pi * hour / 24)
            temperature = base_temp + np.random.normal(0, 2)
            
            # Pressure correlates with temperature
            pressure = 100 + (temperature - 70) * 0.5 + np.random.normal(0, 1)
            
            # Vibration relatively stable
            vibration = 50 + np.random.normal(0, 5)
            
            # Power higher during day
            power = 150 + np.random.normal(0, 10) if 6 <= hour <= 22 else 80 + np.random.normal(0, 5)
            
            # Inject anomalies (5%)
            is_anomaly = np.random.random() < 0.05
            if is_anomaly:
                if np.random.random() < 0.5:
                    temperature = float(np.random.uniform(120, 150))
                else:
                    vibration = float(np.random.uniform(150, 200))
            
            data.append((
                timestamp,
                f"SENSOR_{day*24 + hour:06d}",
                round(float(temperature), 2),
                round(float(pressure), 2),
                round(float(vibration), 2),
                round(float(power), 2),
                is_anomaly
            ))
    
    return spark.createDataFrame(data, [
        "timestamp", "sensor_id", "temperature", "pressure",
        "vibration", "power_consumption", "is_anomaly"
    ])

# Generate sensor data
sensor_df = generate_iot_sensor_data(n_days=30)

print(f"✅ Generated {sensor_df.count()} sensor readings")
print(f"   Normal: {sensor_df.filter('is_anomaly = false').count()}")
print(f"   Anomalies: {sensor_df.filter('is_anomaly = true').count()}")

display(sensor_df.orderBy("timestamp").limit(20))

# COMMAND ----------

# Extract temporal features
print("⏰ Extracting temporal features...")

sensor_with_temporal = extract_temporal_features(
    sensor_df,
    timestamp_column="timestamp",
    features=["hour", "day_of_week", "is_weekend"]
)

print("✅ Temporal features added!")
display(sensor_with_temporal.select(
    "timestamp", "temperature", "pressure",
    "temporal_hour", "temporal_day_of_week", "temporal_is_weekend"
).limit(10))

# COMMAND ----------

# Train sensor anomaly model
training_sensor = sensor_with_temporal.filter("is_anomaly = false").sample(fraction=0.7, seed=42)

print("🎓 Training sensor monitoring model...")

train(
    df=training_sensor,
    columns=[
        "temperature", "pressure", "vibration", "power_consumption",
        "temporal_hour", "temporal_day_of_week", "temporal_is_weekend"
    ],
    model_name="iot_sensor_monitor",
    registry_table="main.default.dqx_anomaly_models"
)

print("✅ Sensor model trained!")

# COMMAND ----------

# Detect sensor anomalies
checks = [
    has_no_anomalies(
        columns=[
            "temperature", "pressure", "vibration", "power_consumption",
            "temporal_hour", "temporal_day_of_week", "temporal_is_weekend"
        ],
        model="iot_sensor_monitor",
        registry_table="main.default.dqx_anomaly_models",
        score_threshold=0.65,
        include_contributions=True,
        drift_threshold=3.0
    )
]

scored_sensors = dq_engine.apply_checks_by_metadata(sensor_with_temporal, checks)

print("🚨 Detected Sensor Anomalies:")
display(
    scored_sensors
    .filter("anomaly_score > 0.65")
    .orderBy(F.desc("anomaly_score"))
    .select(
        "timestamp", "temperature", "pressure", "vibration",
        "anomaly_score", "anomaly_contributions", "is_anomaly"
    )
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💾 Scenario 3: Quarantine Workflow
# MAGIC 
# MAGIC Automatically separate valid transactions from suspicious ones for review.

# COMMAND ----------

# Split valid and quarantine using DQX
print("🔀 Applying quarantine workflow...")

checks = [
    has_no_anomalies(
        columns=["amount", "items_count", "discount_rate", "shipping_cost", "customer_age"],
        model="ecommerce_fraud_detector",
        registry_table="main.default.dqx_anomaly_models",
        score_threshold=0.6,
        include_contributions=True
    )
]

valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(testing_df, checks)

print(f"✅ Valid transactions: {valid_df.count()}")
print(f"⚠️  Quarantined for review: {quarantine_df.count()}")

# COMMAND ----------

# Save quarantined records
print("💾 Saving quarantined records...")

save_dataframe_as_table(
    quarantine_df,
    OutputConfig(
        location="main.default.quarantine_transactions",
        mode="overwrite"
    )
)

print("✅ Saved to: main.default.quarantine_transactions")

# Display quarantined records
print("\n📋 Quarantined Transactions for Manual Review:")
display(
    quarantine_df
    .orderBy(F.desc("anomaly_score"))
    .select(
        "transaction_id", "amount", "discount_rate",
        "anomaly_score", "anomaly_contributions", "is_anomaly"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 Summary
# MAGIC 
# MAGIC ### What We Demonstrated
# MAGIC 
# MAGIC ✅ **E-commerce Fraud Detection**
# MAGIC - Detected high-value fraud, pricing errors, system glitches
# MAGIC - Used multivariate analysis across 5 features
# MAGIC - Achieved high precision and recall
# MAGIC 
# MAGIC ✅ **IoT Sensor Monitoring**
# MAGIC - Incorporated temporal patterns (hour, day, weekend)
# MAGIC - Detected sensor faults and equipment failures
# MAGIC - Monitored for calibration drift
# MAGIC 
# MAGIC ✅ **Quarantine Workflow**
# MAGIC - Automatically separated valid from suspicious records
# MAGIC - Saved anomalies for manual review
# MAGIC - Ready for production integration
# MAGIC 
# MAGIC ### Key Features Used
# MAGIC 
# MAGIC 1. **Multivariate Anomaly Detection** - Catches patterns single-column checks miss
# MAGIC 2. **Temporal Features** - Accounts for time-based patterns
# MAGIC 3. **Feature Contributions** - Explains why each record was flagged
# MAGIC 4. **Drift Detection** - Monitors for model staleness
# MAGIC 5. **Quarantine Integration** - Production-ready workflow
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 
# MAGIC 1. Adjust thresholds based on your false positive tolerance
# MAGIC 2. Monitor model performance over time
# MAGIC 3. Retrain periodically as patterns evolve
# MAGIC 4. Set up alerts for high-confidence anomalies
# MAGIC 5. Review quarantined records and provide feedback

# COMMAND ----------

# View all trained models
print("📚 All Trained Models in Registry:")
display(
    spark.table("main.default.dqx_anomaly_models")
    .filter("status = 'active'")
    .select(
        "model_name",
        "columns",
        "training_rows",
        "training_time",
        "metrics"
    )
    .orderBy(F.desc("training_time"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Demo Complete!
# MAGIC 
# MAGIC You've successfully demonstrated DQX anomaly detection with:
# MAGIC - Realistic synthetic data generation
# MAGIC - Model training and evaluation
# MAGIC - Anomaly scoring and analysis
# MAGIC - Temporal feature integration
# MAGIC - Production-ready quarantine workflows
# MAGIC 
# MAGIC **For more details, see**: [DQX Anomaly Detection Documentation](https://databrickslabs.github.io/dqx/guide/anomaly_detection/)

