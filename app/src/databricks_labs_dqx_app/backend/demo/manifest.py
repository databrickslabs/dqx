"""Pure-data definition of the DQX Studio e-commerce data-quality demo.

This module is the single source of truth for the demo content: the source
tables to generate, a reusable rule set (named to a strict minimal-phrasing
copy contract), the table-to-rule bindings, two data products, governed column
tags, and a 9-week quality "story" (which rules are live in which week, plus
baseline fail rates). It has no side effects and no I/O; later tasks (datagen,
redate, seed orchestrator) consume these constants.

Rule *logic* is stored as a *body* plus *slots* plus *parameters*, and its
shape depends on the rule's ``mode``:

* ``dqx_native`` -> ``body`` is ``{"function", "arguments"}`` where
  ``arguments`` holds ONLY ``{{slot}}`` column placeholders (keyed by the
  check-function argument name). Every SCALAR (non-column) check argument
  lives in ``parameters`` as a :class:`ParamSpec`, mirroring the app's
  canonical dqx_native model (``body.arguments`` = column placeholders,
  ``definition.parameters`` = typed scalar values the authoring UI reads).
* ``sql`` -> ``body`` is ``{"predicate"}`` with ``{{slot}}`` placeholders.
* ``lowcode`` -> ``body`` is ``{"lowcode_ast", ["group_by"]}``; the seed
  orchestrator compiles the AST (via ``lowcode_compile.compile_lowcode_body``)
  into the final stored ``predicate`` / ``sql_query`` / ``merge_columns``,
  exactly as the AI "build with AI" path does.

Each *slot* names a placeholder a binding fills with a real column. A later
task converts these to *RuleDefinition* objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SOURCE_CATALOG_ENV_DEFAULT = "dqx"
SOURCE_SCHEMA = "dqx_studio_demo"
WEEKS_DEFAULT = 9
TIGHTEN_WEEK = 6
# Description applied to the card-validation rule mid-history (at TIGHTEN_WEEK)
# to drive a real rule edit + re-approve so the rule advances to a new published
# version — the "tightened card validation" story beat. A metadata-only change
# (fingerprint excludes descriptive tags), so every binding stays valid.
CARD_RULE_TIGHTENED_DESCRIPTION = "Stored last four card digits are exactly four digits and never blank."
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
class ParamSpec:
    """A scalar (non-column) argument a rule declares as a typed parameter.

    Mirrors the app's :class:`~databricks_labs_dqx_app.backend.registry_models.RuleParameter`
    (name, type, value). For a ``dqx_native`` rule every scalar check argument
    (e.g. ``min_limit``, ``limit``, ``allowed``, ``regex``) is declared here —
    NOT baked into ``body.arguments`` — so the authoring UI reads its value
    from ``definition.parameters`` and a re-save preserves it.

    Args:
        name: parameter name as it appears in the check-function signature.
        type: registry ``ParamType`` value (number|string|list|boolean|regex|ref_table|ref_column).
        value: the concrete scalar value (JSON-shaped: str|float|int|bool|list[str]|None).
    """

    name: str
    type: str
    value: object


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
        body: mode-specific check body. ``dqx_native`` -> ``{"function",
            "arguments"}`` where ``arguments`` holds ONLY ``{{slot}}`` column
            placeholders (scalars live in *parameters*). ``sql`` ->
            ``{"predicate"}``. ``lowcode`` -> ``{"lowcode_ast", ["group_by"]}``
            the seed orchestrator compiles into the final stored body.
        slots: the slots the body declares.
        parameters: scalar (non-column) check arguments, declared as typed
            :class:`ParamSpec` entries. Non-empty only for ``dqx_native`` rules
            whose check function takes scalar arguments.
        slot_tags: optional map of slot name -> governed ``class.*`` tags the slot suggests.
        author_kind: rule provenance — one of ``human``, ``ai_generated`` or
            ``ai_assisted``; spread across the set so the demo shows a realistic
            mix of hand-authored and AI-originated rules.
        polarity: for ``sql``/``lowcode`` rules only, whether the predicate
            describes a passing (``pass``) or failing (``fail``) row. The demo's
            SQL and low-code predicates all describe a VALID row, so they are
            ``pass``. ``dqx_native`` rules leave this ``None``.
    """

    key: str
    name: str
    description: str
    dimension: str
    severity: str
    mode: str
    body: dict[str, object]
    slots: tuple[SlotSpec, ...]
    author_kind: str
    parameters: tuple[ParamSpec, ...] = ()
    slot_tags: dict[str, tuple[str, ...]] = field(default_factory=dict)
    polarity: str | None = None


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
# Reusable rule set — minimal-phrasing names, DQX-shaped bodies, a genuine
# spread across all three authoring modes (dqx_native, lowcode, sql).
#
# * ``dqx_native`` bodies use real DQX check-function names and argument keys
#   from ``src/databricks/labs/dqx/check_funcs.py``. ``arguments`` carries ONLY
#   ``{{slot}}`` column placeholders; every scalar check argument is a typed
#   ``ParamSpec`` in ``parameters`` (the app's canonical model — the authoring
#   UI reads scalar values from ``definition.parameters``).
# * ``lowcode`` bodies carry the re-editable ``lowcode_ast`` (+ optional
#   ``group_by``); the seed orchestrator compiles it to the stored predicate /
#   sql_query via ``lowcode_compile.compile_lowcode_body``. Low-code slots have
#   ``arg_key=None`` — they fill ``{{placeholders}}``, not function arguments —
#   and each row's ``column_ref`` matches a declared slot name.
# * ``sql`` bodies carry a ``{{slot}}``-templated ``predicate``.
#
# Note: DQX has no ``is_not_negative`` check; "amount is not negative" is the
# low-code row ``number >= 0`` (equivalent to the real ``is_not_less_than``
# with limit 0).
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
        author_kind="human",
    ),
    RuleSpec(
        key="nonneg",
        name="Amount is not negative",
        description="Numeric amount is zero or greater.",
        dimension="Accuracy",
        severity="Medium",
        mode="lowcode",
        body={
            "lowcode_ast": {
                "rows": [
                    {"kind": "row", "combinator": None, "column_ref": "number", "operator": ">=", "value": 0},
                ],
                "joins": [],
            },
        },
        slots=(SlotSpec("number", "numeric"),),
        author_kind="human",
        polarity="pass",
    ),
    RuleSpec(
        key="pct_range",
        name="Discount is 0 to 100",
        description="Discount percentage falls between 0 and 100 inclusive.",
        dimension="Accuracy",
        severity="Low",
        mode="lowcode",
        body={
            "lowcode_ast": {
                "rows": [
                    {"kind": "row", "combinator": None, "column_ref": "pct", "operator": "between", "value": [0, 100]},
                ],
                "joins": [],
            },
        },
        slots=(SlotSpec("pct", "numeric"),),
        author_kind="ai_assisted",
        polarity="pass",
    ),
    RuleSpec(
        key="country_set",
        name="Country is a known ISO code",
        description="Country code is one of the supported ISO codes.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={"function": "is_in_list", "arguments": {"column": "{{country}}"}},
        slots=(SlotSpec("country", "text", arg_key="column"),),
        parameters=(
            ParamSpec("allowed", "list", ["US", "GB", "DE", "FR", "ES", "IT", "NL", "CA", "AU", "JP", "IN", "BR"]),
        ),
        author_kind="ai_generated",
        slot_tags={"country": ("class.location",)},
    ),
    RuleSpec(
        key="tier_set",
        name="Account tier is Free, Pro or Enterprise",
        description="Account tier is one of Free, Pro or Enterprise.",
        dimension="Validity",
        severity="Low",
        mode="dqx_native",
        body={"function": "is_in_list", "arguments": {"column": "{{tier}}"}},
        slots=(SlotSpec("tier", "text", arg_key="column"),),
        parameters=(ParamSpec("allowed", "list", ["Free", "Pro", "Enterprise"]),),
        author_kind="ai_generated",
    ),
    RuleSpec(
        key="status_set",
        name="Order status is a known status",
        description="Order status is one of placed, shipped, delivered or cancelled.",
        dimension="Validity",
        severity="Medium",
        mode="lowcode",
        body={
            "lowcode_ast": {
                "rows": [
                    {
                        "kind": "row",
                        "combinator": None,
                        "column_ref": "status",
                        "operator": "in",
                        "value": ["placed", "shipped", "delivered", "cancelled"],
                    },
                ],
                "joins": [],
            },
        },
        slots=(SlotSpec("status", "text"),),
        author_kind="ai_assisted",
        polarity="pass",
    ),
    RuleSpec(
        key="method_set",
        name="Payment method is a known method",
        description="Payment method is one of card, paypal or transfer.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={"function": "is_in_list", "arguments": {"column": "{{method}}"}},
        slots=(SlotSpec("method", "text", arg_key="column"),),
        parameters=(ParamSpec("allowed", "list", ["card", "paypal", "transfer"]),),
        author_kind="ai_generated",
    ),
    RuleSpec(
        key="category_set",
        name="Product category is a known category",
        description="Product category is one of Electronics, Apparel, Home, Grocery or Toys.",
        dimension="Validity",
        severity="Low",
        mode="lowcode",
        body={
            "lowcode_ast": {
                "rows": [
                    {
                        "kind": "row",
                        "combinator": None,
                        "column_ref": "category",
                        "operator": "in",
                        "value": ["Electronics", "Apparel", "Home", "Grocery", "Toys"],
                    },
                ],
                "joins": [],
            },
        },
        slots=(SlotSpec("category", "text"),),
        author_kind="ai_assisted",
        polarity="pass",
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
        author_kind="human",
    ),
    RuleSpec(
        key="unique",
        name="Key is unique",
        description="Key value appears at most once in the table.",
        dimension="Uniqueness",
        severity="High",
        mode="lowcode",
        body={
            "lowcode_ast": {
                "rows": [
                    {
                        "kind": "aggregated",
                        "combinator": None,
                        "aggregate": "count",
                        "column_ref": "key",
                        "operator": "=",
                        "value": 1,
                    },
                ],
                "joins": [],
            },
            "group_by": "{{key}}",
        },
        slots=(SlotSpec("key", "any"),),
        author_kind="human",
        polarity="pass",
    ),
    RuleSpec(
        key="card_format",
        name="Card last-four is four digits",
        description="Stored last four card digits are exactly four digits.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={"function": "regex_match", "arguments": {"column": "{{code}}"}},
        slots=(SlotSpec("code", "text", arg_key="column"),),
        parameters=(ParamSpec("regex", "regex", "^[0-9]{4}$"),),
        author_kind="ai_assisted",
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
        author_kind="human",
        polarity="pass",
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
        author_kind="ai_generated",
        polarity="pass",
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
        author_kind="ai_generated",
        polarity="pass",
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
        author_kind="ai_assisted",
        polarity="pass",
    ),
    # ----------------------------------------------------------------------- #
    # Governed-tag SHOWCASE rules — carry a class.* slot tag but are NOT
    # referenced by any binding below, so the seed creates+approves them into
    # the registry yet leaves them UNAPPLIED. That gives the tag-based
    # apply-rules / suggestion flow a live, un-applied match to demonstrate:
    # each targets the same governed tag as a tagged demo column
    # (class.location -> customers.country_code, class.credit_card ->
    # payments.card_last4), so the "matched tag" surfaces in the apply-rules UI.
    # ----------------------------------------------------------------------- #
    RuleSpec(
        key="iso2_country",
        name="Valid ISO 3166-1 alpha-2 country code",
        description="Country code is a two-letter uppercase ISO 3166-1 alpha-2 code.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={"function": "regex_match", "arguments": {"column": "{{country}}"}},
        slots=(SlotSpec("country", "text", arg_key="column"),),
        parameters=(ParamSpec("regex", "regex", "^[A-Z]{2}$"),),
        author_kind="ai_generated",
        slot_tags={"country": ("class.location",)},
    ),
    RuleSpec(
        key="card_not_null",
        name="Card last-four is present",
        description="Stored last four card digits are not null.",
        dimension="Completeness",
        severity="Medium",
        mode="dqx_native",
        body={"function": "is_not_null", "arguments": {"column": "{{code}}"}},
        slots=(SlotSpec("code", "any", arg_key="column"),),
        author_kind="ai_assisted",
        slot_tags={"code": ("class.credit_card",)},
    ),
    # ----------------------------------------------------------------------- #
    # Submitted-but-not-approved SHOWCASE rule — seeded via create -> submit
    # ONLY (never approved, never embedded, never bound to a column), so it
    # lands in the Review & Approve queue awaiting a human decision. Referenced
    # by no binding below, so it stays an UNMAPPED library draft. Keyed in
    # ``PENDING_APPROVAL_RULE_KEYS`` so the seed orchestrator branches it.
    # ----------------------------------------------------------------------- #
    RuleSpec(
        key="ssn_format",
        name="Valid social security number",
        description="Social security number matches the NNN-NN-NNNN format.",
        dimension="Validity",
        severity="Medium",
        mode="dqx_native",
        body={"function": "regex_match", "arguments": {"column": "{{ssn}}"}},
        slots=(SlotSpec("ssn", "text", arg_key="column"),),
        parameters=(ParamSpec("regex", "regex", r"^\d{3}-\d{2}-\d{4}$"),),
        author_kind="human",
    ),
)

RULES_BY_KEY: dict[str, RuleSpec] = {r.key: r for r in RULES}

# Rule keys seeded as submitted-for-approval only: the seed orchestrator
# creates + submits them (so they sit in the Review & Approve / drafts queue
# awaiting a human decision) but NEVER auto-approves, embeds, or binds them.
# Referenced by no binding, so they stay UNMAPPED library drafts.
PENDING_APPROVAL_RULE_KEYS: frozenset[str] = frozenset({"ssn_format"})

# Source table the demo runs the profiler against so the Profile page shows a
# real ``dq_profiling_results`` row (see ``DemoSeedService._run_profiling``).
PROFILE_DEMO_TABLE = "customers"


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
