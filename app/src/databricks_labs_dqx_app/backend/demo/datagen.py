"""Pure SQL builders for the DQX Studio e-commerce demo source data.

Every function here is pure: it returns a SQL string (or a list of them) and
performs no I/O. A later orchestrator (seed_service) executes the strings via a
*SqlExecutor*. Three concerns live here:

* **Seeded source tables** — deterministic CTAS from ``range(N)`` where a fixed
  fraction of rows carry a controlled data-quality issue, keyed off
  ``pmod(hash(id, salt), 1000) < threshold`` so reruns are byte-for-byte
  identical.
* **Per-week mutations** — full-column ``UPDATE ... SET col = CASE ...``
  statements that drive each column to a target weekly failure rate, realizing
  the 9-week quality story (customers improving, an orders incident, a payments
  tightening step, a shipments bump). Deterministic and idempotent.

Governed column tags are NOT emitted here: the manifest's ``class.*`` tags have
dotted keys that no ``ALTER TABLE ... SET TAGS`` SQL grammar can assign, so the
seeder assigns them through the Unity Catalog Entity Tag Assignments API
instead (see :meth:`~.seed_service.DemoSeedService._assign_column_tag`).

The table columns and row counts are taken from *manifest* so the generated
data lines up exactly with the rule bindings.

Security: every table fully-qualified name is validated with *validate_fqn* and
the table is checked against the known set from *manifest* before it reaches
SQL, and every string literal (such as column comments) is escaped with
*escape_sql_string*. Simple identifiers are emitted unquoted; exotic
catalog/schema names are backtick-quoted via *quote_fqn*.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.demo import manifest
from databricks_labs_dqx_app.backend.demo.manifest import TIGHTEN_WEEK
from databricks_labs_dqx_app.backend.sql_utils import (
    escape_sql_string,
    fqn_needs_quoting,
    quote_fqn,
    quote_ident,
    validate_fqn,
)

# Row counts and known-table set, sourced from the manifest so datagen and the
# rule bindings stay in lock-step.
_ROWS: dict[str, int] = {t.name: t.row_count for t in manifest.TABLES}


def _fqn(catalog: str, schema: str, table: str) -> str:
    """Return a validated, SQL-safe fully-qualified name for a demo table.

    Validates the assembled three-part name (raising *ValueError* on injection
    or a bad identifier) and backtick-quotes it only when it is not a plain
    identifier, so simple demo names stay human-readable in the emitted SQL.

    Args:
        catalog: the catalog name.
        schema: the schema name.
        table: the (already known) table name.

    Returns:
        The validated FQN, quoted only if it needs quoting.
    """
    fqn = f"{catalog}.{schema}.{table}"
    validate_fqn(fqn)
    return quote_fqn(fqn) if fqn_needs_quoting(fqn) else fqn


def _require_known_table(table: str) -> None:
    """Raise *ValueError* unless *table* is one of the demo's known tables.

    This is the injection guard for the table argument: *validate_fqn* alone
    accepts spaces and semicolons inside a part, so the strict allowlist here is
    what rejects a value such as ``customers; DROP TABLE x``.

    Args:
        table: the table name to check.
    """
    if table not in _ROWS:
        raise ValueError(f"unknown demo table: {table!r}; expected one of {sorted(_ROWS)}")


# --------------------------------------------------------------------------- #
# Schema DDL.
# --------------------------------------------------------------------------- #
def create_schema_sql(catalog: str, schema: str) -> str:
    """Return the ``CREATE SCHEMA IF NOT EXISTS`` statement for the demo schema.

    Args:
        catalog: the catalog name.
        schema: the schema name.

    Returns:
        The schema-creation SQL.
    """
    validate_fqn(f"{catalog}.{schema}.x")
    fqn = f"{catalog}.{schema}"
    quoted = quote_fqn(fqn) if fqn_needs_quoting(f"{catalog}.{schema}.x") else fqn
    return f"CREATE SCHEMA IF NOT EXISTS {quoted}"


def drop_schema_sql(catalog: str, schema: str) -> str:
    """Return the ``DROP SCHEMA IF EXISTS ... CASCADE`` statement for the demo schema.

    Args:
        catalog: the catalog name.
        schema: the schema name.

    Returns:
        The schema-drop SQL.
    """
    validate_fqn(f"{catalog}.{schema}.x")
    fqn = f"{catalog}.{schema}"
    quoted = quote_fqn(fqn) if fqn_needs_quoting(f"{catalog}.{schema}.x") else fqn
    return f"DROP SCHEMA IF EXISTS {quoted} CASCADE"


# --------------------------------------------------------------------------- #
# CTAS templates.
#
# Determinism strategy:
#   - id is the row index from range(N).
#   - per-row stable bucket b = pmod(hash(id, salt), 1000) gives 0..999 buckets,
#     so a threshold of < 80 ~= 8.0%. Distinct salts per issue keep overlapping
#     issues independent.
# Placeholders: {fqn} the table FQN; {n} the table's row count; {n_customers}
# and {n_orders} the referenced tables' row counts (unused keys are ignored).
# --------------------------------------------------------------------------- #
_CUSTOMERS_CTAS = """
CREATE OR REPLACE TABLE {fqn}
AS
SELECT
  id AS customer_id,
  concat('First', cast(id AS STRING)) AS first_name,
  concat('Last', cast(id AS STRING)) AS last_name,
  CASE
    WHEN pmod(hash(id, 11), 1000) < 27 THEN ''
    WHEN pmod(hash(id, 11), 1000) < 54 THEN concat('user', cast(id AS STRING), '.example.com')
    WHEN pmod(hash(id, 11), 1000) < 80 THEN concat('user', cast(id AS STRING), '@')
    ELSE concat('user', cast(id AS STRING), '@example.com')
  END AS email,
  concat('+1-555-', lpad(cast(pmod(id, 10000) AS STRING), 4, '0')) AS phone,
  CASE
    WHEN pmod(hash(id, 23), 1000) < 50 THEN 'ZZ'
    ELSE element_at(array('US','GB','DE','FR','ES','IT','NL','CA','AU','JP','IN','BR'), cast(pmod(id, 12) + 1 AS INT))
  END AS country_code,
  CASE
    WHEN pmod(hash(id, 31), 1000) < 30 THEN 'Platinum'
    ELSE element_at(array('Free','Pro','Enterprise'), cast(pmod(id, 3) + 1 AS INT))
  END AS account_tier,
  CASE
    WHEN pmod(hash(id, 47), 1000) < 20 THEN current_timestamp() + make_interval(0, 0, 0, cast(pmod(id, 200) + 1 AS INT))
    ELSE current_timestamp() - make_interval(0, 0, 0, cast(pmod(id, 730) AS INT))
  END AS created_at,
  (pmod(id, 7) <> 0) AS is_active
FROM range({n})
""".strip()

_ORDERS_CTAS = """
CREATE OR REPLACE TABLE {fqn}
AS
SELECT
  id AS order_id,
  CASE
    WHEN pmod(hash(id, 11), 1000) < 8 THEN CAST(NULL AS BIGINT)
    WHEN pmod(hash(id, 11), 1000) < 15 THEN {n_customers} + pmod(id, 100000)
    ELSE pmod(id, {n_customers})
  END AS customer_id,
  CASE
    WHEN pmod(hash(id, 17), 1000) < 10 THEN current_timestamp() + make_interval(0, 0, 0, cast(pmod(id, 90) + 1 AS INT))
    ELSE current_timestamp() - make_interval(0, 0, 0, cast(pmod(id, 365) AS INT))
  END AS order_ts,
  CASE
    WHEN pmod(hash(id, 23), 1000) < 10 THEN -1 * (pmod(id, 200) + 1) * 1.0
    WHEN pmod(hash(id, 23), 1000) < 20 THEN 0.0
    WHEN pmod(hash(id, 23), 1000) < 25 THEN 9999999.99
    ELSE round(5 + pmod(id, 49500) / 100.0, 2)
  END AS amount,
  element_at(array('USD','EUR','GBP'), cast(pmod(id, 3) + 1 AS INT)) AS currency,
  CASE
    WHEN pmod(hash(id, 31), 1000) < 20 THEN 'unknown'
    ELSE element_at(array('placed','shipped','delivered','cancelled'), cast(pmod(id, 4) + 1 AS INT))
  END AS status,
  CASE
    WHEN pmod(hash(id, 41), 1000) < 5 THEN 150.0
    WHEN pmod(hash(id, 41), 1000) < 10 THEN -10.0
    ELSE cast(pmod(id, 41) AS DOUBLE)
  END AS discount_pct
FROM range({n})
""".strip()

_PAYMENTS_CTAS = """
CREATE OR REPLACE TABLE {fqn}
AS
WITH base AS (
  SELECT
    id AS payment_id,
    CASE
      WHEN pmod(hash(id, 11), 1000) < 10 THEN {n_orders} + pmod(id, 100000)
      ELSE pmod(id, {n_orders})
    END AS order_id_raw,
    pmod(id, {n_orders}) AS ref_order_idx
  FROM range({n})
)
SELECT
  payment_id,
  order_id_raw AS order_id,
  CASE
    WHEN pmod(hash(payment_id, 53), 1000) < 15 THEN round(5 + pmod(ref_order_idx, 49500) / 100.0, 2) + 13.37
    ELSE round(5 + pmod(ref_order_idx, 49500) / 100.0, 2)
  END AS amount,
  CASE
    WHEN pmod(hash(payment_id, 23), 1000) < 20 THEN 'bitcoin'
    ELSE element_at(array('card','paypal','transfer'), cast(pmod(payment_id, 3) + 1 AS INT))
  END AS method,
  CASE
    WHEN pmod(hash(payment_id, 31), 1000) < 15 THEN ''
    WHEN pmod(hash(payment_id, 31), 1000) < 30 THEN 'XXXX'
    ELSE lpad(cast(pmod(payment_id, 10000) AS STRING), 4, '0')
  END AS card_last4,
  current_timestamp() - make_interval(0, 0, 0, cast(pmod(payment_id, 365) AS INT)) AS paid_at
FROM base
""".strip()

_PRODUCTS_CTAS = """
CREATE OR REPLACE TABLE {fqn}
AS
SELECT
  CASE
    WHEN pmod(hash(id, 11), 1000) < 10 THEN concat('SKU-', lpad(cast(pmod(id, 50) AS STRING), 6, '0'))
    ELSE concat('SKU-', lpad(cast(id AS STRING), 6, '0'))
  END AS sku,
  CASE
    WHEN pmod(hash(id, 17), 1000) < 10 THEN CAST(NULL AS STRING)
    ELSE concat('Product ', cast(id AS STRING))
  END AS name,
  CASE
    WHEN pmod(hash(id, 23), 1000) < 20 THEN -1 * round(1 + pmod(id, 500) / 10.0, 2)
    ELSE round(1 + pmod(id, 50000) / 100.0, 2)
  END AS price,
  CASE
    WHEN pmod(hash(id, 31), 1000) < 30 THEN 'Misc'
    ELSE element_at(array('Electronics','Apparel','Home','Grocery','Toys'), cast(pmod(id, 5) + 1 AS INT))
  END AS category
FROM range({n})
""".strip()

_SHIPMENTS_CTAS = """
CREATE OR REPLACE TABLE {fqn}
AS
WITH base AS (
  SELECT
    id AS shipment_id,
    CASE
      WHEN pmod(hash(id, 11), 1000) < 10 THEN {n_orders} + pmod(id, 100000)
      ELSE pmod(id, {n_orders})
    END AS order_id_raw,
    pmod(id, {n_orders}) AS ref_order_idx
  FROM range({n})
),
withts AS (
  SELECT
    *,
    current_timestamp() - make_interval(0, 0, 0, cast(pmod(ref_order_idx, 365) AS INT)) AS order_ts
  FROM base
)
SELECT
  shipment_id,
  order_id_raw AS order_id,
  element_at(array('UPS','FedEx','DHL','USPS'), cast(pmod(shipment_id, 4) + 1 AS INT)) AS carrier,
  CASE
    WHEN pmod(hash(shipment_id, 17), 1000) < 20 THEN CAST(NULL AS STRING)
    WHEN pmod(hash(shipment_id, 17), 1000) < 40 THEN 'AB'
    ELSE concat('TRK', lpad(cast(shipment_id AS STRING), 10, '0'))
  END AS tracking_no,
  CASE
    WHEN pmod(hash(shipment_id, 23), 1000) < 10 THEN order_ts - make_interval(0, 0, 0, cast(pmod(shipment_id, 5) + 1 AS INT))
    ELSE order_ts + make_interval(0, 0, 0, cast(pmod(shipment_id, 5) + 1 AS INT))
  END AS shipped_at,
  CASE
    WHEN pmod(hash(shipment_id, 31), 1000) < 15
      THEN (order_ts + make_interval(0, 0, 0, cast(pmod(shipment_id, 5) + 1 AS INT))) - make_interval(0, 0, 0, cast(pmod(shipment_id, 3) + 1 AS INT))
    ELSE (order_ts + make_interval(0, 0, 0, cast(pmod(shipment_id, 5) + 1 AS INT))) + make_interval(0, 0, 0, cast(pmod(shipment_id, 7) + 1 AS INT))
  END AS delivered_at
FROM withts
""".strip()

_TABLE_CTAS: dict[str, str] = {
    "customers": _CUSTOMERS_CTAS,
    "orders": _ORDERS_CTAS,
    "payments": _PAYMENTS_CTAS,
    "products": _PRODUCTS_CTAS,
    "shipments": _SHIPMENTS_CTAS,
}


def build_create_table_sql(table: str, catalog: str, schema: str) -> str:
    """Return the deterministic CTAS that generates a demo source table.

    Args:
        table: one of the demo's known table names.
        catalog: the catalog name.
        schema: the schema name.

    Returns:
        A ``CREATE OR REPLACE TABLE ... AS SELECT ... FROM range(N)`` statement.

    Raises:
        ValueError: if *table* is not a known demo table or the FQN is invalid.
    """
    _require_known_table(table)
    fqn = _fqn(catalog, schema, table)
    return _TABLE_CTAS[table].format(
        fqn=fqn,
        n=_ROWS[table],
        n_customers=_ROWS["customers"],
        n_orders=_ROWS["orders"],
    )


def build_column_comment_sql(table: str, column: str, comment: str, catalog: str, schema: str) -> str:
    """Return an ``ALTER COLUMN ... COMMENT`` statement documenting a seeded issue.

    Args:
        table: one of the demo's known table names.
        column: the column to comment.
        comment: the human-readable comment text.
        catalog: the catalog name.
        schema: the schema name.

    Returns:
        The column-comment SQL, with the comment escaped as a string literal.

    Raises:
        ValueError: if *table* is not a known demo table or the FQN is invalid.
    """
    _require_known_table(table)
    fqn = _fqn(catalog, schema, table)
    return f"ALTER TABLE {fqn} ALTER COLUMN {quote_ident(column)} COMMENT '{escape_sql_string(comment)}'"


# --------------------------------------------------------------------------- #
# Per-week mutations.
#
# Each week rebuilds a table's issue columns at a new threshold so the realized
# failure rate moves to a target, driving the 9-week quality story. Mutations
# are deterministic (pmod(hash(pk, salt), 1000) < threshold) and idempotent:
# each fully overwrites the affected column, so reruns converge regardless of
# prior week.
# --------------------------------------------------------------------------- #
_ISO = "array('US','GB','DE','FR','ES','IT','NL','CA','AU','JP','IN','BR')"
_FUTURE = "timestampadd(DAY, 30, current_timestamp())"


def _past(pk: str, span: int) -> str:
    """Return a SQL expression for a stable past timestamp, varied per row."""
    return f"timestampadd(DAY, -cast(pmod({pk}, {span}) AS INT), current_timestamp())"


def _thr(rate: float) -> int:
    """Convert a rate in ``[0, 1]`` to an integer threshold out of 1000."""
    return max(0, min(1000, int(round(rate * 1000))))


def _mut(fqn: str, col: str, pk: str, salt: int, rate: float, bad: str, good: str) -> str:
    """Return an ``UPDATE`` that sets *col* to *bad* for ~*rate* of rows, else *good*.

    The row is selected deterministically by ``pmod(hash(pk, salt), 1000)`` and
    the whole column is overwritten so reruns converge regardless of order.
    """
    return (
        f"UPDATE {fqn} SET {col} = CASE "
        f"WHEN pmod(hash({pk}, {salt}), 1000) < {_thr(rate)} THEN {bad} ELSE {good} END"
    )


def _clamp(value: float) -> float:
    """Clamp a fail level into the visible ``[0.005, 0.90]`` band.

    The 0.90 ceiling is deliberate: a per-column mutation rate is multiplied by
    up to 1.25 before landing here, and the seeder's validation gate hard-fails
    any check at or above ``_MISFIRE_RATE`` (0.985) as a mis-bound rule. Capping
    the *legitimate* seeded failure at 0.90 keeps a "bad week" dramatic while
    leaving clear headroom below the misfire threshold, so a genuinely
    mis-bound rule (which flags ~100% of rows) is still the only thing that
    trips the gate.
    """
    return max(0.005, min(0.90, value))


def _jitter(week: int, salt: int, amp: float) -> float:
    """Return a deterministic per-table run-to-run wobble in roughly ``[-amp, amp]``."""
    return (((week * salt + 7) % 17) - 8) / 8.0 * amp


def _fail_levels(week: int, weeks: int) -> dict[str, float]:
    """Return the per-table fail level for *week* of a *weeks*-long story.

    Encodes the story beats: customers improving, an orders incident mid-run, a
    payments decline with a tightening step at *TIGHTEN_WEEK*, chronically bad
    products, and a variable early shipments bump. Deterministic.

    Args:
        week: the zero-based week index.
        weeks: the total number of weeks in the story.

    Returns:
        A map of table name to its target fail level for the week.
    """
    frac = week / max(weeks - 1, 1)
    incident = max(0.0, 1 - abs(week - (weeks - 1) * 0.42) / 2.0)
    shump = max(0.0, 1 - abs(week - (weeks - 1) * 0.18) / 2.0)
    return {
        "customers": _clamp(0.74 - 0.60 * frac + _jitter(week, 13, 0.13)),
        "orders": _clamp(0.06 + 0.80 * incident + _jitter(week, 29, 0.12)),
        "payments": _clamp(
            0.16 + 0.38 * frac + (0.14 if week >= TIGHTEN_WEEK else 0.0) + _jitter(week, 23, 0.12)
        ),
        "products": _clamp(0.64 + _jitter(week, 19, 0.20)),
        "shipments": _clamp(0.18 + 0.40 * shump + _jitter(week, 31, 0.16)),
    }


def _mutate_customers(fqn: str, f: float) -> list[str]:
    """Return the customers column-mutation statements for fail level *f*."""
    return [
        _mut(fqn, "first_name", "customer_id", 43, _clamp(f * 1.15),
             "CAST(NULL AS STRING)", "concat('First', cast(customer_id AS STRING))"),
        _mut(fqn, "last_name", "customer_id", 44, _clamp(f * 1.10),
             "CAST(NULL AS STRING)", "concat('Last', cast(customer_id AS STRING))"),
        _mut(fqn, "country_code", "customer_id", 23, _clamp(f * 1.25),
             "'ZZ'", f"element_at({_ISO}, cast(pmod(customer_id, 12) + 1 AS INT))"),
        _mut(fqn, "account_tier", "customer_id", 41, _clamp(f * 0.6),
             "'Gold'", "element_at(array('Free','Pro','Enterprise'), cast(pmod(customer_id, 3) + 1 AS INT))"),
        _mut(fqn, "created_at", "customer_id", 53, _clamp(f * 0.9),
             _FUTURE, _past("customer_id", 3650)),
        _mut(fqn, "phone", "customer_id", 57, _clamp(f * 1.05),
             "CASE WHEN pmod(hash(customer_id, 57), 2) = 0 THEN CAST(NULL AS STRING) ELSE '12' END",
             "concat('+1555', lpad(cast(pmod(customer_id, 10000) AS STRING), 4, '0'))"),
        _mut(fqn, "is_active", "customer_id", 59, _clamp(f * 0.7),
             "CAST(NULL AS BOOLEAN)", "(pmod(customer_id, 2) = 0)"),
    ]


def _mutate_orders(fqn: str, f: float, n_customers: int) -> list[str]:
    """Return the orders column-mutation statements for fail level *f*."""
    return [
        _mut(fqn, "customer_id", "order_id", 67, _clamp(f * 0.7),
             "CAST(NULL AS BIGINT)", f"cast(pmod(order_id, {n_customers}) + 1 AS BIGINT)"),
        _mut(fqn, "order_ts", "order_id", 71, _clamp(f * 0.8),
             _FUTURE, _past("order_id", 1460)),
        _mut(fqn, "amount", "order_id", 23, _clamp(f * 1.0),
             "-1 * (pmod(order_id, 200) + 1) * 1.0", "round(5 + pmod(order_id, 49500) / 100.0, 2)"),
        _mut(fqn, "discount_pct", "order_id", 61, _clamp(f * 0.9),
             "150", "cast(pmod(order_id, 60) AS INT)"),
        _mut(fqn, "status", "order_id", 31, _clamp(f * 1.15),
             "'unknown'",
             "element_at(array('placed','shipped','delivered','cancelled'), cast(pmod(order_id, 4) + 1 AS INT))"),
    ]


def _mutate_payments(fqn: str, f: float, n_orders: int) -> list[str]:
    """Return the payments column-mutation statements for fail level *f*."""
    return [
        _mut(fqn, "order_id", "payment_id", 83, _clamp(f * 0.5),
             "CAST(NULL AS BIGINT)", f"cast(pmod(payment_id, {n_orders}) + 1 AS BIGINT)"),
        _mut(fqn, "amount", "payment_id", 89, _clamp(f * 0.6),
             "-1 * (pmod(payment_id, 100) + 1) * 1.0", "round(5 + pmod(payment_id, 49500) / 100.0, 2)"),
        _mut(fqn, "paid_at", "payment_id", 97, _clamp(f * 0.7),
             _FUTURE, _past("payment_id", 1460)),
        _mut(fqn, "method", "payment_id", 29, _clamp(f * 1.1),
             "'bitcoin'", "element_at(array('card','paypal','transfer'), cast(pmod(payment_id, 3) + 1 AS INT))"),
        _mut(fqn, "card_last4", "payment_id", 31, _clamp(f * 1.0),
             "'XXXX'", "lpad(cast(pmod(payment_id, 10000) AS STRING), 4, '0')"),
    ]


def _mutate_products(fqn: str, f: float) -> list[str]:
    """Return the products column-mutation statements for fail level *f*."""
    return [
        _mut(fqn, "name", "sku", 47, _clamp(f * 1.1),
             "CAST(NULL AS STRING)", "concat('Product-', sku)"),
        _mut(fqn, "price", "sku", 23, _clamp(f * 1.0),
             "-1 * round(1 + pmod(length(sku), 500) / 10.0, 2)", "round(1 + pmod(length(sku), 50000) / 100.0, 2)"),
        _mut(fqn, "category", "sku", 37, _clamp(f * 1.25),
             "'Misc'",
             "element_at(array('Electronics','Apparel','Home','Grocery','Toys'), cast(pmod(hash(sku, 99), 5) + 1 AS INT))"),
    ]


def _mutate_shipments(fqn: str, f: float, n_orders: int) -> list[str]:
    """Return the shipments column-mutation statements for fail level *f*."""
    return [
        _mut(fqn, "order_id", "shipment_id", 101, _clamp(f * 0.5),
             "CAST(NULL AS BIGINT)", f"cast(pmod(shipment_id, {n_orders}) + 1 AS BIGINT)"),
        _mut(fqn, "tracking_no", "shipment_id", 17, _clamp(f * 1.1),
             "CASE WHEN pmod(hash(shipment_id, 17), 2) = 0 THEN CAST(NULL AS STRING) ELSE 'AB' END",
             "concat('TRK', lpad(cast(shipment_id AS STRING), 10, '0'))"),
        _mut(fqn, "shipped_at", "shipment_id", 103, _clamp(f * 0.6),
             _FUTURE, _past("shipment_id", 1460)),
        _mut(fqn, "delivered_at", "shipment_id", 71, _clamp(f * 0.9),
             "timestampadd(DAY, -2, shipped_at)",
             "timestampadd(DAY, cast(pmod(shipment_id, 10) + 1 AS INT), shipped_at)"),
    ]


def build_mutation_sql(table: str, week: int, weeks: int, catalog: str, schema: str) -> list[str]:
    """Return the per-week column-mutation statements for a demo table.

    Each statement is a full-column ``UPDATE`` that drives one column to its
    target failure rate for *week*. Deterministic and idempotent.

    Args:
        table: one of the demo's known table names.
        week: the zero-based week index.
        weeks: the total number of weeks in the story.
        catalog: the catalog name.
        schema: the schema name.

    Returns:
        The list of ``UPDATE`` statements for the table's mutated columns.

    Raises:
        ValueError: if *table* is not a known demo table or the FQN is invalid.
    """
    _require_known_table(table)
    fqn = _fqn(catalog, schema, table)
    f = _fail_levels(week, weeks)[table]
    n_customers = _ROWS["customers"]
    n_orders = _ROWS["orders"]
    if table == "customers":
        return _mutate_customers(fqn, f)
    if table == "orders":
        return _mutate_orders(fqn, f, n_customers)
    if table == "payments":
        return _mutate_payments(fqn, f, n_orders)
    if table == "products":
        return _mutate_products(fqn, f)
    return _mutate_shipments(fqn, f, n_orders)


def build_baseline_reset_sql(catalog: str, schema: str) -> list[str]:
    """Return the week-0 mutations that reset every table to a known baseline.

    Running these makes the source deterministic regardless of any prior
    mutations, so a fresh run history is reproducible.

    Args:
        catalog: the catalog name.
        schema: the schema name.

    Returns:
        The flattened week-0 ``UPDATE`` statements across all demo tables.
    """
    stmts: list[str] = []
    for table in _ROWS:
        stmts.extend(build_mutation_sql(table, week=0, weeks=1, catalog=catalog, schema=schema))
    return stmts
