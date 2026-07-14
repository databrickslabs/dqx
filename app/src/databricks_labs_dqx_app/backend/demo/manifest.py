"""Pure-data definition of the DQX Studio e-commerce data-quality demo.

This module is the single source of truth for the demo content: the source
tables to generate, a reusable rule set (named to a strict minimal-phrasing
copy contract), the table-to-rule bindings, two data products, governed column
tags, and a 9-week quality "story" (which rules are live in which week, plus
baseline fail rates). It has no side effects and no I/O; later tasks (datagen,
redate, seed orchestrator) consume these constants.

Rule *logic* is stored as a *body* plus *slots*: *body* mirrors DQX's
declarative check shape (``dqx_native`` -> ``{"function", "arguments"}`` with
``{{slot}}`` placeholders; ``sql`` -> ``{"predicate"}``), and each *slot* names
a placeholder a binding fills with a real column. A later task converts these
to *RuleDefinition* objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SOURCE_CATALOG_ENV_DEFAULT = "dqx"
SOURCE_SCHEMA = "dqx_studio_demo"
WEEKS_DEFAULT = 9
TIGHTEN_WEEK = 6
# Uniqueness reports one failed row per row in a duplicated-key group, not one
# per distinct duplicate key. ~1% of ids collapse onto earlier keys, so the
# band is generous but far below the ~all-rows count a misfiring rule produces.
UNIQUE_EXPECT_ROWS: tuple[int, int] = (0, 700)


@dataclass(frozen=True)
class SlotSpec:
    """A placeholder in a rule body that a binding fills with a real column.

    Args:
        name: placeholder name used in the body (``{{name}}``) and binding groups.
        family: column family the slot accepts (numeric|text|temporal|boolean|array|any).
        arg_key: the check-function argument the slot fills for *dqx_native* rules
            (e.g. *column*, *columns*); None for *sql* rules with no function argument.
    """

    name: str
    family: str
    arg_key: str | None = None


@dataclass(frozen=True)
class RuleSpec:
    """A reusable, parameterised data-quality rule.

    Args:
        key: stable handle referenced by bindings and the lifecycle story.
        name: short human-readable label (minimal phrasing, no leading article, <=80 chars).
        description: one-sentence description ending in a single period, no leading article.
        dimension: one of the six DQ dimensions.
        severity: one of Low, Medium, High, Critical.
        mode: dqx_native | lowcode | sql.
        body: declarative check body with ``{{slot}}`` placeholders.
        slots: the slots the body declares.
        slot_tags: optional map of slot name -> governed ``class.*`` tags the slot suggests.
    """

    key: str
    name: str
    description: str
    dimension: str
    severity: str
    mode: str
    body: dict[str, object]
    slots: tuple[SlotSpec, ...]
    slot_tags: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class TableSpec:
    """A source table the demo generates.

    Args:
        name: table name (unqualified).
        row_count: number of rows datagen creates.
        primary_key: the table's primary-key column.
        columns: every column datagen creates, in order.
    """

    name: str
    row_count: int
    primary_key: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class BindingSpec:
    """A table's rule bindings.

    Args:
        table: the bound table name.
        display_name: human-readable label for the binding.
        mappings: rule-key -> tuple of mapping groups; each group maps a rule's
            slot names to real columns (one check per group).
    """

    table: str
    display_name: str
    mappings: dict[str, tuple[dict[str, str], ...]]


@dataclass(frozen=True)
class DataProductSpec:
    """A named grouping of demo tables.

    Args:
        name: product name.
        description: one-line product description.
        members: member table names.
    """

    name: str
    description: str
    members: tuple[str, ...]


@dataclass(frozen=True)
class ColumnTagSpec:
    """A governed column tag applied to a demo column.

    Args:
        table: table the tagged column belongs to.
        column: the tagged column.
        tag: the governed tag (``class.*`` namespace).
    """

    table: str
    column: str
    tag: str


# --------------------------------------------------------------------------- #
# Source tables (columns must match what datagen creates).
# --------------------------------------------------------------------------- #
TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        name="customers",
        row_count=50_000,
        primary_key="customer_id",
        columns=(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "country_code",
            "account_tier",
            "created_at",
            "is_active",
        ),
    ),
    TableSpec(
        name="orders",
        row_count=200_000,
        primary_key="order_id",
        columns=("order_id", "customer_id", "order_ts", "amount", "currency", "status", "discount_pct"),
    ),
    TableSpec(
        name="payments",
        row_count=180_000,
        primary_key="payment_id",
        columns=("payment_id", "order_id", "amount", "method", "card_last4", "paid_at"),
    ),
    TableSpec(
        name="products",
        row_count=5_000,
        primary_key="sku",
        columns=("sku", "name", "price", "category"),
    ),
    TableSpec(
        name="shipments",
        row_count=150_000,
        primary_key="shipment_id",
        columns=("shipment_id", "order_id", "carrier", "tracking_no", "shipped_at", "delivered_at"),
    ),
)


# --------------------------------------------------------------------------- #
# Reusable rule set — minimal-phrasing names, DQX-shaped bodies.
#
# Bodies use real DQX check-function names and argument keys from
# ``src/databricks/labs/dqx/check_funcs.py``. Note: DQX has no ``is_not_negative``
# check; "amount is not negative" is expressed with the real ``is_not_less_than``
# (limit 0).
# --------------------------------------------------------------------------- #
RULES: tuple[RuleSpec, ...] = (
    RuleSpec(
        key="present",
        name="Value is present",
        description="Value is not null.",
        dimension="Completeness",
        severity="High",
        mode="dqx_native",
        body={"function": "is_not_null", "arguments": {"column": "{{value}}"}},
        slots=(SlotSpec("value", "any", arg_key="column"),),
    ),
    RuleSpec(
        key="nonneg",
        name="Amount is not negative",
        description="Numeric amount is zero or greater.",
        dimension="Accuracy",
        severity="Medium",
        mode="dqx_native",
        body={"function": "is_not_less_than", "arguments": {"column": "{{number}}", "limit": 0}},
        slots=(SlotSpec("number", "numeric", arg_key="column"),),
    ),
    RuleSpec(
        key="pct_range",
        name="Discount is 0 to 100",
        description="Discount percentage falls between 0 and 100 inclusive.",
        dimension="Accuracy",
        severity="Low",
        mode="dqx_native",
        body={
            "function": "is_in_range",
            "arguments": {"column": "{{pct}}", "min_limit": 0, "max_limit": 100},
        },
        slots=(SlotSpec("pct", "numeric", arg_key="column"),),
    ),
    RuleSpec(
        key="country_set",
        name="Country is a known ISO code",
        description="Country code is one of the supported ISO codes.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={
            "function": "is_in_list",
            "arguments": {
                "column": "{{country}}",
                "allowed": ["US", "GB", "DE", "FR", "ES", "IT", "NL", "CA", "AU", "JP", "IN", "BR"],
            },
        },
        slots=(SlotSpec("country", "text", arg_key="column"),),
        slot_tags={"country": ("class.location",)},
    ),
    RuleSpec(
        key="tier_set",
        name="Account tier is Free, Pro or Enterprise",
        description="Account tier is one of Free, Pro or Enterprise.",
        dimension="Validity",
        severity="Low",
        mode="dqx_native",
        body={"function": "is_in_list", "arguments": {"column": "{{tier}}", "allowed": ["Free", "Pro", "Enterprise"]}},
        slots=(SlotSpec("tier", "text", arg_key="column"),),
    ),
    RuleSpec(
        key="status_set",
        name="Order status is a known status",
        description="Order status is one of placed, shipped, delivered or cancelled.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={
            "function": "is_in_list",
            "arguments": {"column": "{{status}}", "allowed": ["placed", "shipped", "delivered", "cancelled"]},
        },
        slots=(SlotSpec("status", "text", arg_key="column"),),
    ),
    RuleSpec(
        key="method_set",
        name="Payment method is a known method",
        description="Payment method is one of card, paypal or transfer.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={
            "function": "is_in_list",
            "arguments": {"column": "{{method}}", "allowed": ["card", "paypal", "transfer"]},
        },
        slots=(SlotSpec("method", "text", arg_key="column"),),
    ),
    RuleSpec(
        key="category_set",
        name="Product category is a known category",
        description="Product category is one of Electronics, Apparel, Home, Grocery or Toys.",
        dimension="Validity",
        severity="Low",
        mode="dqx_native",
        body={
            "function": "is_in_list",
            "arguments": {
                "column": "{{category}}",
                "allowed": ["Electronics", "Apparel", "Home", "Grocery", "Toys"],
            },
        },
        slots=(SlotSpec("category", "text", arg_key="column"),),
    ),
    RuleSpec(
        key="not_future",
        name="Timestamp is not in the future",
        description="Event timestamp is no later than now.",
        dimension="Timeliness",
        severity="Medium",
        mode="dqx_native",
        body={"function": "is_not_in_future", "arguments": {"column": "{{ts}}"}},
        slots=(SlotSpec("ts", "temporal", arg_key="column"),),
    ),
    RuleSpec(
        key="unique",
        name="Key is unique",
        description="Key value appears at most once in the table.",
        dimension="Uniqueness",
        severity="High",
        mode="dqx_native",
        body={"function": "is_unique", "arguments": {"columns": ["{{key}}"]}},
        slots=(SlotSpec("key", "any", arg_key="columns"),),
    ),
    RuleSpec(
        key="card_format",
        name="Card last-four is four digits",
        description="Stored last four card digits are exactly four digits.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={"function": "regex_match", "arguments": {"column": "{{code}}", "regex": "^[0-9]{4}$"}},
        slots=(SlotSpec("code", "text", arg_key="column"),),
        slot_tags={"code": ("class.credit_card",)},
    ),
    RuleSpec(
        key="min_len",
        name="Tracking number is long enough",
        description="Tracking number is at least five characters.",
        dimension="Validity",
        severity="Low",
        mode="sql",
        body={"predicate": "length({{text}}) >= 5"},
        slots=(SlotSpec("text", "text"),),
    ),
    RuleSpec(
        key="end_after_start",
        name="End is not before start",
        description="End timestamp is on or after start timestamp.",
        dimension="Consistency",
        severity="Medium",
        mode="sql",
        body={"predicate": "{{end_ts}} >= {{start_ts}}"},
        slots=(SlotSpec("start_ts", "temporal"), SlotSpec("end_ts", "temporal")),
    ),
    RuleSpec(
        key="card_when_card",
        name="Card details present for card payments",
        description="Card last four digits are four digits whenever the payment method is card.",
        dimension="Consistency",
        severity="High",
        mode="sql",
        body={"predicate": "{{method}} <> 'card' OR {{card_last4}} RLIKE '^[0-9]{4}$'"},
        slots=(SlotSpec("method", "text"), SlotSpec("card_last4", "text")),
    ),
    RuleSpec(
        key="amount_and_discount",
        name="Amount positive and discount valid",
        description="Amount is above zero and any discount percentage is between 0 and 100.",
        dimension="Accuracy",
        severity="Critical",
        mode="sql",
        body={
            "predicate": "{{amount}} > 0 AND ({{discount_pct}} IS NULL OR {{discount_pct}} BETWEEN 0 AND 100)",
        },
        slots=(SlotSpec("amount", "numeric"), SlotSpec("discount_pct", "numeric")),
    ),
)

RULES_BY_KEY: dict[str, RuleSpec] = {r.key: r for r in RULES}


# --------------------------------------------------------------------------- #
# Bindings — which rules apply to which table, with slot -> column mappings.
#
# The same rule key on several tables (present, not_future, nonneg, unique) is
# the reuse story. Referential/foreign-key rules are intentionally omitted: the
# DQX engine runs row checks against the source table only and won't inline a
# join, so an FK rule would misfire.
# --------------------------------------------------------------------------- #
BINDINGS: tuple[BindingSpec, ...] = (
    BindingSpec(
        table="customers",
        display_name="Customers",
        mappings={
            "present": (
                {"value": "customer_id"},
                {"value": "first_name"},
                {"value": "last_name"},
                {"value": "phone"},
                {"value": "is_active"},
            ),
            "country_set": ({"country": "country_code"},),
            "tier_set": ({"tier": "account_tier"},),
            "not_future": ({"ts": "created_at"},),
            "min_len": ({"text": "phone"},),
            "unique": ({"key": "customer_id"},),
        },
    ),
    BindingSpec(
        table="orders",
        display_name="Orders",
        mappings={
            "present": ({"value": "customer_id"},),
            "not_future": ({"ts": "order_ts"},),
            "nonneg": ({"number": "amount"},),
            "pct_range": ({"pct": "discount_pct"},),
            "status_set": ({"status": "status"},),
            "amount_and_discount": ({"amount": "amount", "discount_pct": "discount_pct"},),
        },
    ),
    BindingSpec(
        table="payments",
        display_name="Payments",
        mappings={
            "present": ({"value": "order_id"},),
            "nonneg": ({"number": "amount"},),
            "not_future": ({"ts": "paid_at"},),
            "method_set": ({"method": "method"},),
            "card_format": ({"code": "card_last4"},),
            "card_when_card": ({"method": "method", "card_last4": "card_last4"},),
        },
    ),
    BindingSpec(
        table="products",
        display_name="Products",
        mappings={
            "present": ({"value": "name"},),
            "nonneg": ({"number": "price"},),
            "category_set": ({"category": "category"},),
            "unique": ({"key": "sku"},),
        },
    ),
    BindingSpec(
        table="shipments",
        display_name="Shipments",
        mappings={
            "present": ({"value": "order_id"}, {"value": "tracking_no"}),
            "min_len": ({"text": "tracking_no"},),
            "not_future": ({"ts": "shipped_at"},),
            "end_after_start": ({"start_ts": "shipped_at", "end_ts": "delivered_at"},),
        },
    ),
)


# --------------------------------------------------------------------------- #
# Data products.
# --------------------------------------------------------------------------- #
DATA_PRODUCTS: tuple[DataProductSpec, ...] = (
    DataProductSpec(
        name="Customer 360",
        description="Customer, order and payment data for the customer view.",
        members=("customers", "orders", "payments"),
    ),
    DataProductSpec(
        name="Fulfillment",
        description="Order, product and shipment data for the fulfilment flow.",
        members=("orders", "products", "shipments"),
    ),
)


# --------------------------------------------------------------------------- #
# Governed column tags — a class.* tag applied to a demo column. Tag names are
# REAL governed tags (from SHOW GOVERNED TAGS): class.location and
# class.credit_card both exist in the standard PII governed-tag set. The tagged
# columns line up with the rules' slot_tags (country_set -> class.location on
# country_code; card_format -> class.credit_card on card_last4) so tag-driven
# rule suggestion has real targets to match.
# --------------------------------------------------------------------------- #
COLUMN_TAGS: tuple[ColumnTagSpec, ...] = (
    ColumnTagSpec(table="customers", column="country_code", tag="class.location"),
    ColumnTagSpec(table="payments", column="card_last4", tag="class.credit_card"),
)


# --------------------------------------------------------------------------- #
# The 9-week story.
#
# RULE_LIFECYCLE: (table, rule_key) -> (start_week, end_week); active window is
# the half-open interval [start, end). A key absent here is active throughout.
# Story beats: customers coverage grows (tier at wk2, uniqueness at wk3) while
# the freshness check is retired at wk6; orders adds a discount-range check at
# wk3; payments tightens card checks at wk4 and drops freshness at wk5; the
# shipments tracking-length check is the headline new rule at wk5.
# --------------------------------------------------------------------------- #
RULE_LIFECYCLE: dict[tuple[str, str], tuple[int, int]] = {
    ("customers", "tier_set"): (2, 99),
    ("customers", "unique"): (3, 99),
    ("customers", "not_future"): (0, 6),
    ("orders", "pct_range"): (3, 99),
    ("orders", "not_future"): (0, 5),
    ("payments", "not_future"): (0, 5),
    ("payments", "card_when_card"): (4, 99),
    ("shipments", "min_len"): (5, 99),
    ("shipments", "end_after_start"): (0, 7),
}

# Baseline week-0 fail rate by (table, column). The big weekly swings (the
# orders incident, the payments tightening step) live in the datagen mutation
# logic; these are the week-0 starting points only.
EXPECT: dict[tuple[str, str], float] = {
    ("customers", "country_code"): 0.40,
    ("customers", "created_at"): 0.03,
    ("orders", "status"): 0.05,
    ("orders", "amount"): 0.03,
    ("payments", "method"): 0.16,
    ("payments", "card_last4"): 0.03,
    ("products", "category"): 0.25,
    ("products", "price"): 0.02,
    ("products", "name"): 0.06,
}


def active_mapping(binding: BindingSpec, week: int) -> dict[str, tuple[dict[str, str], ...]]:
    """Return the binding's rule-key -> mapping-group subset active at *week*.

    Filters the binding's mappings by the add/retire windows in
    *RULE_LIFECYCLE*. A rule with no lifecycle entry is active in every week.

    Args:
        binding: the binding whose mappings to filter.
        week: the zero-based week index.

    Returns:
        The subset of *binding.mappings* whose lifecycle window contains *week*.
    """
    out: dict[str, tuple[dict[str, str], ...]] = {}
    for rule_key, groups in binding.mappings.items():
        start, end = RULE_LIFECYCLE.get((binding.table, rule_key), (0, 99))
        if start <= week < end:
            out[rule_key] = groups
    return out
