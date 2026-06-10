# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Fashion & Retail Industry Accelerator Demo
# MAGIC ## Product Catalog Quality & Merchandising Compliance
# MAGIC
# MAGIC This demo showcases DQX data quality rules tailored for the **Fashion and Retail industry**, with a focus on **product catalog integrity, pricing compliance, and merchandising standards**.
# MAGIC
# MAGIC ### Use Case: Product Catalog Ingestion & Quality Gate
# MAGIC
# MAGIC Process incoming product catalog uploads from vendors and internal merchandising teams and automatically:
# MAGIC * **Quarantine invalid products** (errors) that must be fixed before going live on the storefront
# MAGIC * **Flag merchandising issues** (warnings) for buyer review while still allowing products into the pipeline
# MAGIC * **Enforce data integrity** to ensure downstream inventory, pricing, and recommendation systems receive clean data
# MAGIC
# MAGIC ### Implemented Data Quality Checks
# MAGIC
# MAGIC **1. Custom Pricing Intelligence (Suspicious Pricing Detection)**
# MAGIC * **Pennies-off psychological pricing**: Prices ending in .95, .97, .98, .99 are expected — but extreme penny-off patterns (e.g., $99.01) may indicate data entry errors
# MAGIC * **Suspiciously round wholesale prices**: Perfectly round prices ($100.00, $500.00) in retail contexts often indicate placeholder or draft pricing
# MAGIC * **Extreme margin outliers**: Unit cost exceeding retail price (negative margin) or margin above 95% (likely data error)
# MAGIC
# MAGIC **2. Critical Field Validation (Errors)**
# MAGIC * Required fields: `sku`, `product_name`, `category` must not be null or empty
# MAGIC * SKU format: Must follow standard pattern (e.g., `TSHIRT-BLK-M`, `JEANS-BLU-32`)
# MAGIC * Hex color code: Must be valid CSS hex color (#RRGGBB or #RGB)
# MAGIC * Positive price: Listing price must be greater than zero
# MAGIC
# MAGIC **3. Business Logic Validation (Errors)**
# MAGIC * **Margin consistency**: Unit cost must not exceed retail price (negative margin)
# MAGIC * **Weight plausibility**: Shipping weight must be within plausible bounds for apparel (0.01–25 kg)
# MAGIC
# MAGIC **4. Data Quality Standards (Warnings)**
# MAGIC * **Apparel sizing**: Must follow standard sizing codes (XS, S, M, L, XL, XXL, OS, or numeric like 32-L)
# MAGIC * **Category taxonomy**: Must belong to known product categories
# MAGIC * **Season code**: Must follow format (e.g., SS25, FW24, AW25, RS25)
# MAGIC * **Brand-category consistency**: Luxury brands should not have products priced below $50
# MAGIC
# MAGIC ### Key DQX Features Demonstrated
# MAGIC
# MAGIC * **Dual-Routing Split Behavior**: Warnings go to BOTH valid and invalid DataFrames (monitoring without blocking)
# MAGIC * **Custom Check Functions**: Industry-specific pricing intelligence integrated with built-in checks
# MAGIC * **Centralized Rule Management**: Rules stored in Delta tables for versioning and governance
# MAGIC * **Single-Pass Execution**: All checks applied in one data scan for optimal performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX

# COMMAND ----------

# DBTITLE 1,Install DQX Library

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

import os

workspace_root_path = os.getcwd()
quality_rules_path = f"{workspace_root_path}/quality_checks"

# Cleanup existing DQ Rules files, if already exists
if os.path.exists(quality_rules_path):
    for filename in os.listdir(quality_rules_path):
        file_path = os.path.join(quality_rules_path, filename)
        # Only delete files, not subdirectories
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Deleted: {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Best Practice: Pin DQX Version
# MAGIC
# MAGIC To ensure consistent behavior and avoid unexpected issues from automatic upgrades, it is highly recommended to always pin DQX to a specific version in your production pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Catalog and Schema
# MAGIC
# MAGIC Specify the catalog and schema where the quality rules table will be stored.

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema for Demo
default_catalog_name = "main"
default_schema_name = "default"

dbutils.widgets.text("demo_catalog", default_catalog_name, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema_name, "Schema Name")

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

print(f"Selected Catalog: {catalog}")
print(f"Selected Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Sample Fashion Product Catalog Data
# MAGIC
# MAGIC We'll create a DataFrame simulating a typical product catalog upload from vendors or a merchandising system sync.

# COMMAND ----------

from pyspark.sql import SparkSession, Row

# Create a comprehensive sample DataFrame simulating a fashion product catalog ingestion
products = [
    # ── Valid products ──────────────────────────────────────────────────────────
    Row(sku="TSHIRT-BLK-M",    product_name="Classic Crew Tee",         category="Tops",        brand="BasicsLab",   size_code="M",    color_hex="#000000", retail_price=29.99,  unit_cost=8.50,   weight_kg=0.18, season_code="SS25"),
    Row(sku="JEANS-BLU-32",    product_name="Slim Fit Denim",           category="Bottoms",     brand="DenimCo",     size_code="32-L", color_hex="#1A3A5C", retail_price=89.99,  unit_cost=28.00,  weight_kg=0.65, season_code="FW24"),
    Row(sku="HOODIE-RED-XL",   product_name="Oversized Hoodie",         category="Outerwear",   brand="StreetPulse", size_code="XL",   color_hex="#C0392B", retail_price=65.00,  unit_cost=22.00,  weight_kg=0.55, season_code="AW25"),
    Row(sku="DRESS-PNK-S",     product_name="Midi Wrap Dress",          category="Dresses",     brand="Atelier Lux", size_code="S",    color_hex="#F5A6C8", retail_price=185.00, unit_cost=52.00,  weight_kg=0.35, season_code="SS25"),
    Row(sku="SNEAKER-WHT-42",  product_name="Retro Runner",             category="Footwear",    brand="StepForward", size_code="42",   color_hex="#F8F8F8", retail_price=120.00, unit_cost=38.00,  weight_kg=0.75, season_code="RS25"),
    Row(sku="SCARF-GLD-OS",    product_name="Silk Evening Scarf",       category="Accessories", brand="Atelier Lux", size_code="OS",   color_hex="#D4AF37", retail_price=95.00,  unit_cost=30.00,  weight_kg=0.08, season_code="FW24"),
    Row(sku="BLAZER-NVY-L",    product_name="Tailored Wool Blazer",     category="Outerwear",   brand="Atelier Lux", size_code="L",    color_hex="#1B2631", retail_price=320.00, unit_cost=110.00, weight_kg=0.90, season_code="AW25"),
    Row(sku="SHORTS-KHK-M",    product_name="Chino Shorts",             category="Bottoms",     brand="BasicsLab",   size_code="M",    color_hex="#C8B88A", retail_price=45.00,  unit_cost=12.00,  weight_kg=0.28, season_code="SS25"),

    # ── Suspicious pricing patterns (warnings) ─────────────────────────────────
    Row(sku="COAT-BLK-L",      product_name="Trench Coat",              category="Outerwear",   brand="BasicsLab",   size_code="L",    color_hex="#0D0D0D", retail_price=200.00, unit_cost=60.00,  weight_kg=1.20, season_code="FW24"),  # Warn: Perfectly round retail price
    Row(sku="POLO-GRN-M",      product_name="Piqué Polo Shirt",         category="Tops",        brand="BasicsLab",   size_code="M",    color_hex="#2E8B57", retail_price=500.00, unit_cost=15.00,  weight_kg=0.22, season_code="SS25"),  # Warn: Round price + extreme 97% margin
    Row(sku="BAG-TAN-OS",      product_name="Leather Tote Bag",         category="Accessories", brand="Atelier Lux", size_code="OS",   color_hex="#D2B48C", retail_price=350.00, unit_cost=12.00,  weight_kg=0.60, season_code="AW25"),  # Warn: Round price + extreme 96.6% margin

    # ── Invalid sizing (warning) ────────────────────────────────────────────────
    Row(sku="SOCKS-WHT-PK3",   product_name="Cotton Ankle Socks 3-Pack",category="Accessories", brand="BasicsLab",   size_code="HUGE", color_hex="#FFFFFF", retail_price=12.50,  unit_cost=3.00,   weight_kg=0.10, season_code="SS25"),  # Warn: Non-standard size "HUGE"

    # ── Invalid hex color (error) ───────────────────────────────────────────────
    Row(sku="HAT-GRN-OS",      product_name="Bucket Hat",               category="Accessories", brand="StreetPulse", size_code="OS",   color_hex="GREEN",  retail_price=25.00,  unit_cost=7.00,   weight_kg=0.12, season_code="SS25"),  # Error: "GREEN" is not a valid hex color
    Row(sku="VEST-CRM-M",      product_name="Knit Vest",                category="Tops",        brand="BasicsLab",   size_code="M",    color_hex="#GGHHII",retail_price=55.00,  unit_cost=18.00,  weight_kg=0.30, season_code="FW24"),  # Error: Invalid hex chars

    # ── Negative / zero price (error) ───────────────────────────────────────────
    Row(sku="BELT-BRN-36",     product_name="Leather Belt",             category="Accessories", brand="DenimCo",     size_code="36",   color_hex="#8B4513", retail_price=-15.00, unit_cost=5.00,   weight_kg=0.15, season_code="SS25"),  # Error: Negative price
    Row(sku="TIE-BLU-OS",      product_name="Silk Tie",                 category="Accessories", brand="Atelier Lux", size_code="OS",   color_hex="#2C3E50", retail_price=0.00,   unit_cost=25.00,  weight_kg=0.05, season_code="FW24"),  # Error: Zero price

    # ── Null required fields (error) ────────────────────────────────────────────
    Row(sku=None,              product_name="Mystery Item",             category="Tops",        brand="BasicsLab",   size_code="S",    color_hex="#FFC0CB", retail_price=40.00,  unit_cost=12.00,  weight_kg=0.20, season_code="SS25"),  # Error: Null SKU
    Row(sku="JACKET-GRY-L",    product_name=None,                       category="Outerwear",   brand="StreetPulse", size_code="L",    color_hex="#808080", retail_price=110.00, unit_cost=35.00,  weight_kg=0.70, season_code="AW25"),  # Error: Null product name

    # ── Invalid category (warning) ──────────────────────────────────────────────
    Row(sku="LAMP-WHT-OS",     product_name="Table Lamp",               category="HomeGoods",   brand="BasicsLab",   size_code="OS",   color_hex="#EEEEEE", retail_price=75.00,  unit_cost=20.00,  weight_kg=2.50, season_code="SS25"),  # Warn: Category not in taxonomy

    # ── Invalid season code (warning) ───────────────────────────────────────────
    Row(sku="CARDIGAN-BRG-M",  product_name="Cashmere Cardigan",        category="Tops",        brand="Atelier Lux", size_code="M",    color_hex="#800020", retail_price=245.00, unit_cost=85.00,  weight_kg=0.40, season_code="XMAS"),  # Warn: Non-standard season code

    # ── Negative margin: cost > price (error) ───────────────────────────────────
    Row(sku="BOOT-BLK-40",     product_name="Chelsea Boot",             category="Footwear",    brand="StepForward", size_code="40",   color_hex="#1C1C1C", retail_price=89.00,  unit_cost=120.00, weight_kg=1.10, season_code="AW25"),  # Error: Cost exceeds price

    # ── Implausible weight (error) ──────────────────────────────────────────────
    Row(sku="RING-SLV-OS",     product_name="Sterling Silver Ring",     category="Accessories", brand="Atelier Lux", size_code="OS",   color_hex="#C0C0C0", retail_price=150.00, unit_cost=45.00,  weight_kg=0.005,season_code="RS25"),  # Error: Weight below 0.01 kg minimum

    # ── Brand-price inconsistency (warning) ─────────────────────────────────────
    Row(sku="TEE-WHT-XS",      product_name="Basic White Tee",          category="Tops",        brand="Atelier Lux", size_code="XS",   color_hex="#FAFAFA", retail_price=15.00,  unit_cost=4.00,   weight_kg=0.16, season_code="SS25"),  # Warn: Luxury brand priced under $50

    # ── More valid products for pipeline flow ───────────────────────────────────
    Row(sku="PARKA-OLV-XL",    product_name="Expedition Parka",         category="Outerwear",   brand="StreetPulse", size_code="XL",   color_hex="#556B2F", retail_price=275.00, unit_cost=95.00,  weight_kg=1.40, season_code="FW24"),
]

products_table = f"{catalog}.{schema}.fashion_products"

if spark.catalog.tableExists(products_table) and spark.table(products_table).count() > 0:
    print(f"Table {products_table} already exists with demo data. Skipping data generation")
else:
    products_df = spark.createDataFrame(products)
    products_df.write.mode("overwrite").saveAsTable(products_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Dataset
# MAGIC
# MAGIC ### Fashion Product Catalog Dataset
# MAGIC
# MAGIC | Column Name    | Data Type | Description                                                    | Example Value     |
# MAGIC |----------------|-----------|----------------------------------------------------------------|-------------------|
# MAGIC | `sku`          | string    | Stock Keeping Unit — `CATEGORY-COLOR-SIZE` format              | TSHIRT-BLK-M      |
# MAGIC | `product_name` | string    | Human-readable product description                             | Classic Crew Tee  |
# MAGIC | `category`     | string    | Product taxonomy category                                      | Tops              |
# MAGIC | `brand`        | string    | Brand attribution                                              | BasicsLab         |
# MAGIC | `size_code`    | string    | Apparel sizing (XS–XXL, OS, or numeric like 32-L)              | M                 |
# MAGIC | `color_hex`    | string    | CSS hex color for storefront display                           | #000000           |
# MAGIC | `retail_price` | double    | Listed retail price (must be positive)                         | 29.99             |
# MAGIC | `unit_cost`    | double    | Vendor unit cost (must not exceed retail price)                | 8.50              |
# MAGIC | `weight_kg`    | double    | Shipping weight in kilograms (0.01–25 kg)                      | 0.18              |
# MAGIC | `season_code`  | string    | Merchandising season code (e.g., SS25, FW24, AW25, RS25)       | SS25              |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some Sample Product Catalog Data

# COMMAND ----------

# DBTITLE 1,Products Bronze Table
products_df = spark.read.table(products_table)
print("=== Product Catalog Data Sample ===")
display(products_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto-Infer Quality Rules with DQProfiler
# MAGIC
# MAGIC Before defining rules manually, DQX can **automatically infer data quality rules** from the data profile. This is useful for bootstrapping a quality rule set for a new dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1**: Read raw data and instantiate DQX

# COMMAND ----------

import yaml
from pprint import pprint

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig, TableChecksStorageConfig

ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2**: Run DQProfiler to infer quality rules

# COMMAND ----------

# DBTITLE 1,Profile Data and Infer Quality Rules
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(products_df)

generator = DQGenerator(ws)
inferred_checks = generator.generate_dq_rules(profiles)

print("=== Inferred DQ Checks ===\n")
for idx, check in enumerate(inferred_checks):
    print(f"========Check {idx} ==========\n")
    pprint(check)

# COMMAND ----------

# DBTITLE 1,Save Inferred Rules to Workspace File
fashion_inferred_rules_yaml = f"{quality_rules_path}/fashion_inferred_dq_rules.yml"
dq_engine.save_checks(inferred_checks, config=WorkspaceFileChecksStorageConfig(location=fashion_inferred_rules_yaml))
displayHTML(f'<a href="/#workspace{fashion_inferred_rules_yaml}" target="_blank">Fashion Inferred Quality Rules YAML</a>')

# COMMAND ----------

# DBTITLE 1,Save Inferred Rules to Delta Table
fashion_inferred_rules_table = f"{catalog}.{schema}.fashion_inferred_quality_rules"
dq_engine.save_checks(inferred_checks, config=TableChecksStorageConfig(location=fashion_inferred_rules_table, run_config_name="fashion_inferred"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3**: Apply Inferred Quality Rules to Input Data

# COMMAND ----------

# Load checks from workspace file
inferred_quality_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=fashion_inferred_rules_yaml))

# Apply checks on input data
valid_inferred_df, quarantined_inferred_df = dq_engine.apply_checks_by_metadata_and_split(products_df, inferred_quality_checks)

print("=== Products Quarantined by Inferred Rules ===")
display(quarantined_inferred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bring Your Own Rule: Suspicious Pricing Pattern Detection
# MAGIC
# MAGIC This section demonstrates how to extend DQX with a **custom quality rule**. The workflow follows 3 steps:
# MAGIC 1. **Define** the custom rule function
# MAGIC 2. **Add** the rule to the YAML definition
# MAGIC 3. **Apply** the DQ rules on input data
# MAGIC
# MAGIC For this demo, we define a custom pricing intelligence rule that flags suspicious pricing patterns common in retail product catalogs:
# MAGIC * **Perfectly round retail prices** (e.g., exactly \$100.00, \$200.00) which often indicate placeholder or draft pricing that was never finalized
# MAGIC * **Extreme margin outliers** — margins above 95% typically indicate a data entry error (e.g., cost entered in the wrong currency)
# MAGIC * **Negative margins** — unit cost exceeding retail price signals a pricing or cost error

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Define the Custom Rule Function

# COMMAND ----------

# DBTITLE 1,Custom Industry Check: Suspicious Pricing Pattern Detection
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition, get_normalized_column_and_expr
from databricks.labs.dqx.rule import register_rule

@register_rule("row")
def is_suspicious_pricing_pattern(
    price_column: str | Column,
    cost_column: str | Column,
    round_price_threshold: float = 100.0,
    max_margin_pct: float = 95.0,
) -> Column:
    """
    Detects suspicious pricing patterns common in fashion/retail product catalogs:
    1. Perfectly round retail prices above threshold (e.g., $100.00, $200.00, $500.00)
       — often indicates placeholder/draft pricing
    2. Extreme margin percentages above max_margin_pct (e.g., 95%+)
       — likely a data entry error (wrong currency, missing digit, etc.)
    3. Negative margins (cost > price)
       — pricing or cost error

    These patterns may indicate catalog data quality issues requiring merchandising review.
    """
    price_norm, price_expr_str, price_expr = get_normalized_column_and_expr(price_column)
    _cost_norm, cost_expr_str, cost_expr = get_normalized_column_and_expr(cost_column)

    # Check 1: Perfectly round retail prices above threshold
    is_round = (
        (price_expr >= round_price_threshold)
        & (price_expr == F.floor(price_expr))
        & ((price_expr % 50 == 0) | (price_expr % 100 == 0))
    )

    # Check 2: Extreme margin (margin% = (price - cost) / price * 100)
    margin_pct = ((price_expr - cost_expr) / price_expr) * 100
    extreme_margin = (price_expr > 0) & (cost_expr > 0) & (margin_pct > max_margin_pct)

    # Check 3: Negative margin (cost exceeds price)
    negative_margin = (price_expr > 0) & (cost_expr > price_expr)

    suspicious = is_round | extreme_margin | negative_margin

    return make_condition(
        suspicious,
        F.concat(
            F.lit(f"Suspicious pricing: '{price_expr_str}'="),
            price_expr.cast("string"),
            F.lit(f", '{cost_expr_str}'="),
            cost_expr.cast("string"),
            F.lit(" (possible placeholder, margin outlier, or cost/price inversion)"),
        ),
        f"{price_norm}_suspicious_pricing_pattern",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Add the Custom Rule to YAML Definition
# MAGIC
# MAGIC We define all quality checks — both built-in and custom — in a single YAML configuration.

# COMMAND ----------

import yaml

# Define industry-specific checks in YAML format
fashion_checks_yaml = f"""
# ──── 1. Critical Fields: Must not be null or empty ────────────────────────────
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - sku
    - product_name
    - category
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 2. SKU Format Validation ────────────────────────────────────────────────
# Standard pattern: CATEGORY-COLOR-SIZE (e.g., TSHIRT-BLK-M, JEANS-BLU-32)
- criticality: error
  check:
    function: regex_match
    arguments:
      column: sku
      regex: "^[A-Z]{{2,10}}-[A-Z]{{2,4}}-[A-Z0-9]{{1,4}}$"
    name: sku_format_valid
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 3. Hex Color Code Validation ────────────────────────────────────────────
# Must be valid CSS hex color: #RRGGBB or #RGB
- criticality: error
  check:
    function: regex_match
    arguments:
      column: color_hex
      regex: "^#([A-Fa-f0-9]{{6}}|[A-Fa-f0-9]{{3}})$"
    name: valid_css_hex_color
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 4. Listing Price: Must be positive ──────────────────────────────────────
- criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: retail_price
      limit: 0.01
    name: positive_listing_price
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 5. Suspicious Pricing Pattern (Custom Check) ───────────────────────────
- name: suspicious_pricing_pattern
  criticality: warn
  check:
    function: is_suspicious_pricing_pattern
    arguments:
      price_column: retail_price
      cost_column: unit_cost
      round_price_threshold: 100.0
      max_margin_pct: 95.0
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 6. Apparel Sizing Validation ────────────────────────────────────────────
# Accepted formats: XS, S, M, L, XL, XXL, OS, or numeric (32, 42, 32-L)
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: size_code
      regex: "^(XS|S|M|L|XL|XXL|OS|[0-9]{{2}}(-[A-Z])?)$"
    name: standard_apparel_sizing
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 7. Product Category Taxonomy ────────────────────────────────────────────
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: category
      allowed: ["Tops", "Bottoms", "Dresses", "Outerwear", "Footwear", "Accessories", "Activewear", "Swimwear", "Loungewear"]
    name: valid_product_category
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 8. Season Code Format ──────────────────────────────────────────────────
# Format: SS25 (Spring/Summer), FW24 (Fall/Winter), AW25 (Autumn/Winter), RS25 (Resort)
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: season_code
      regex: "^(SS|FW|AW|RS|PF|CR)[0-9]{{2}}$"
    name: valid_season_code
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 9. Shipping Weight Plausibility ────────────────────────────────────────
# Apparel and accessories typically weigh between 0.01 kg and 25 kg
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: weight_kg
      min_limit: 0.01
      max_limit: 25.0
    name: weight_within_bounds
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 10. Margin Consistency: Cost must not exceed Price ─────────────────────
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: "unit_cost <= retail_price OR retail_price <= 0"
    name: cost_does_not_exceed_price
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules

# ──── 11. Brand-Price Consistency: Luxury brands must be priced >= $50 ───────
- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: >
        NOT (
          brand IN ('Atelier Lux')
          AND retail_price < 50
        )
    name: luxury_brand_minimum_price
  user_metadata:
    version: v1
    location: {catalog}.{schema}.fashion_quality_rules
"""

checks = yaml.safe_load(fashion_checks_yaml)

# COMMAND ----------

# DBTITLE 1,Validate Checks
status = DQEngine.validate_checks(
    checks,
    custom_check_functions={'is_suspicious_pricing_pattern': is_suspicious_pricing_pattern}
)
print(status)
assert not status.has_errors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Best Practice: Centralized Rule Management
# MAGIC
# MAGIC Instead of hardcoding rules in every pipeline, DQX recommends maintaining them in a **centralized Delta table**. This enables versioning, governance, sharing across teams, and easier discoverability.
# MAGIC
# MAGIC **Recommended Practices:**
# MAGIC * **Access Control**: Grant read-only access to users; restrict write access to service principals.
# MAGIC * **Granularity**: Prioritize row-level checks for distributed performance.
# MAGIC * **One-Pass Execution**: Apply all checks (row, dataset, anomaly) in a single pass to minimize redundant data scans.

# COMMAND ----------

# DBTITLE 1,Centralize Rules in Delta Table
fashion_rules_table = f"{catalog}.{schema}.fashion_quality_rules"

# Note: Custom check functions (like is_suspicious_pricing_pattern) cannot be saved to Delta tables.
# Only built-in checks are persisted. Custom checks must be applied directly from the YAML list.

# Filter out custom checks for table storage (keep only built-in checks)
built_in_checks = [check for check in checks if check.get('check', {}).get('function') != 'is_suspicious_pricing_pattern']

# 1. Save built-in checks to a Delta table
dq_engine.save_checks(
    built_in_checks, 
    config=TableChecksStorageConfig(location=fashion_rules_table, run_config_name="fashion_prod_v1")
)

# 2. Load checks from the Delta table
quality_checks = dq_engine.load_checks(
    config=TableChecksStorageConfig(location=fashion_rules_table, run_config_name="fashion_prod_v1")
)

# 3. Add the custom check back to the loaded checks for runtime application
custom_pricing_check = next(check for check in checks if check.get('check', {}).get('function') == 'is_suspicious_pricing_pattern')
quality_checks.append(custom_pricing_check)

# COMMAND ----------

# DBTITLE 1,Save Rules to Workspace File (Alternative Storage)
# DQX also supports storing rules as workspace YAML files
fashion_rules_yaml = f"{quality_rules_path}/fashion_quality_rules.yml"
dq_engine.save_checks(built_in_checks, config=WorkspaceFileChecksStorageConfig(location=fashion_rules_yaml))

# Display a link to the saved checks file
displayHTML(f'<a href="/#workspace{fashion_rules_yaml}" target="_blank">Fashion Quality Rules YAML</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Apply Checks & Quarantine Invalid Products
# MAGIC
# MAGIC We'll use `apply_checks_by_metadata_and_split` to process the **loaded** checks.
# MAGIC
# MAGIC **Custom Function Handling**: 
# MAGIC * **Better practice**: Pass only specific functions: `{'is_suspicious_pricing_pattern': is_suspicious_pricing_pattern}`
# MAGIC * **Production best practice**: Package custom checks as an importable module instead of notebook-defined functions
# MAGIC
# MAGIC **Split Behavior**:
# MAGIC * Records with **`error`** criticality violations → go **only** to `invalid_df` (quarantined)
# MAGIC * Records with **`warn`** criticality violations → go to **BOTH** `valid_df` and `invalid_df` (allows processing while flagging for review)
# MAGIC * This dual-routing for warnings enables merchandising review without blocking catalog publication
# MAGIC * For different behavior, use `apply_checks_by_metadata()` and filter manually

# COMMAND ----------

# DQX Split Behavior:
# - Records with 'error' criticality violations → ONLY in invalid_df (quarantined)
# - Records with 'warn' criticality violations → BOTH valid_df AND invalid_df (dual-routing)
#
# This design allows merchandising issues (warnings) to be reviewed in the quarantine dataset
# while still flowing through the pipeline to the storefront.

valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
    products_df, 
    quality_checks, 
    custom_check_functions={'is_suspicious_pricing_pattern': is_suspicious_pricing_pattern}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valid Products — Ready for Storefront
# MAGIC Products that pass all error-level checks. Warning-flagged products still appear here for downstream processing.

# COMMAND ----------

display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Invalid / Quarantined Products — Merchandising Review Queue
# MAGIC Data quality issues are captured in the invalid DataFrame via the `_errors` and `_warnings` array columns mapping directly to our named checks.

# COMMAND ----------

display(invalid_df)

# COMMAND ----------

# DBTITLE 1,Persist Quarantine Table
quarantine_table = f"{catalog}.{schema}.fashion_products_quarantine"
invalid_df.write.mode("overwrite").saveAsTable(quarantine_table)
print(f"Quarantined products saved to {quarantine_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Quality on Pre-Configured Dashboard
# MAGIC
# MAGIC When you deploy DQX as [`workspace tool`](https://databrickslabs.github.io/dqx/docs/installation/#dqx-installation-as-a-tool-in-a-databricks-workspace), it automatically generates a Quality Dashboard. <br> You can open the dashboard using Databricks CLI: `databricks labs dqx open-dashboards`
