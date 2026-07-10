"""Build + provision the DQX Studio Genie space over the DQ score views.

Faithful port of dqlake's ``materialiser/genie_space.py``, re-grounded on
this app's score objects. Pure builders (:func:`build_serialized_space` /
:func:`build_create_payload`) are unit-tested; :func:`ensure_dq_genie_space`
does find-or-create-by-title via the raw Genie REST API
(``/api/2.0/genie/spaces``) using the app's SERVICE-PRINCIPAL
WorkspaceClient, then stores the space id in the ``dq_genie_space_id``
app setting.

Identity + permission model (WHY the SP, and why aggregates only):
the Genie space is SP-owned — every question asked through the space runs
with the app service principal's credentials, not the asking user's, so
Unity Catalog permissions of the caller are NOT enforced inside the space.
The space therefore points ONLY at the SP-owned AGGREGATE objects in the
app's main schema and never at row-level data:

- ``mv_dq_scores`` (UC metric view)   — pass rates / failed + total tests
  per table, run, rule, dimension, severity (read measures with MEASURE()).
- ``v_dq_check_results``              — one row per run x table x check,
  carrying error/warning counts, input_row_count, run_mode, and the
  AS-OF-RUN attribution (severity, dimension, criticality, mapped columns).
- ``v_dq_check_attribution``          — the frozen per-run rendered rule
  set (checks_json) exploded to one row per run x table x check.

``dq_quarantine_records`` (and any other raw-row object) is deliberately
EXCLUDED: exposing it through an SP-owned space would leak row-level data
across catalog boundaries. Row-level failing records stay app-only, gated
by the OBO permission checks on the results endpoints.

Product scoping: there is no data-product view. The chat UI prefixes
questions with a context preamble — ``(Table: <fqn>)`` or
``(Data product: <name> — tables: fqn1, fqn2, ...)`` — and the space
instructions route on that preamble.

Idempotency: a config hash of the serialized space is stored alongside the
space id. Unchanged hash -> no-op; changed hash -> PATCH the space in
place (on PATCH failure the new hash is NOT persisted so the next startup
retries); missing id -> find-or-create by title prefix (Databricks appends
a timestamp to the title on create). Best-effort throughout — never raises
out of the app lifespan.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
from collections.abc import Callable

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.score_view_service import (
    ATTRIBUTION_VIEW_NAME,
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
)
from databricks_labs_dqx_app.backend.sql_utils import quote_object_fqn

logger = logging.getLogger(__name__)

SPACE_TITLE = "DQX Studio — DQ Results"
SPACE_DESCRIPTION = "Ask about data-quality scores, pass rates, and failing rules."

# Settings keys (dq_app_settings) — same keys as dqlake so the semantics port 1:1.
SETTING_SPACE_ID = "dq_genie_space_id"
SETTING_CONFIG_HASH = "dq_genie_space_config_hash"
SETTING_STATUS = "dq_genie_space_status"

# Status values surfaced to the UI.
STATUS_PROVISIONING = "provisioning"
STATUS_READY = "ready"
STATUS_ERROR = "error"

# The pre-canned chip questions. These ARE the requirements: every one must
# get a grounded answer from a space dataset. The wording is polarity-neutral
# ("changed", not "decreased"). Row-level questions from dqlake ("show me the
# rows that failed", "failing rows with the most rules failed") are DROPPED —
# the SP-owned space exposes aggregates only (see the module docstring).
SAMPLE_QUESTIONS = [
    "What is the current data quality score?",
    "How many tests failed in the latest run?",
    "Which rules are failing?",
    "Which columns have the most failures?",
    "What are my most severe issues right now?",
    "Which tables have the lowest pass rate?",
    "Which quality dimensions are weakest?",
    "How has the score changed over recent runs?",
    "How has my DQ score by severity been changing over time?",
    "What is driving my changes in score over time?",
    "Why did my DQ score change since the last run?",
    "Why has my score by dimension changed?",
    "What is the biggest factor affecting my DQ score?",
    "How many draft runs happened recently?",
]

# The steward brief. The API concatenates content[] WITHOUT separators, so
# every element ends with "\n". Max one text_instruction per space. The SQL
# snippets + example SQL do the heavy lifting; the prose covers the rules
# those structures can't encode: grain, friendly names, run_mode defaults,
# context routing, diagnosis, and honesty about what the data can't show.
TEXT_INSTRUCTIONS = [
    (
        "You are answering questions from a data steward about the data quality of their tables. "
        "Lead with a short plain-language summary of what you found — the key numbers and what they "
        "mean — and follow with the supporting breakdown. When a lower level of detail explains a "
        "headline number (a rule, a quality dimension, a severity, a column), include it and name "
        "the specific contributors.\n"
    ),
    (
        "A test is one record-level evaluation of one check. Pass rate is "
        "1 - SUM(failed_tests) / SUM(total_tests), computed at the test grain. Read every "
        "metric-view measure with MEASURE(). Report failures as a share of tests run, with the "
        "denominator — \"1,250 of 50,000 tests failed (2.5%)\" — and avoid bare counts, since they "
        "mean little on their own. Do not average pass rates across runs or tables; recompute from "
        "the underlying sums.\n"
    ),
    (
        "Refer to things by their human names: the fully-qualified table name and the run "
        "timestamp. Wrap fully-qualified names and identifiers containing underscores in backticks "
        "so they render literally. Internal identifiers (run ids, rule fingerprints) support joins "
        "but do not belong in answers.\n"
    ),
    (
        "Severity is one of Critical, High, Medium, or Low — present in that order, leading with "
        "Critical. Quality dimensions (Completeness, Validity, and so on) are the steward's "
        "business framing; prefer them when summarising what kind of quality problem exists.\n"
    ),
    (
        "Results carry a run_mode of published or draft. Answer from published runs unless the "
        "question explicitly asks about drafts, and say which you used when it matters.\n"
    ),
    (
        "The message may name its subject: `(Table: <fqn>)` scopes to that table; "
        "`(Data product: <name> — tables: ...)` scopes to those member tables, and the product's "
        "headline score is the mean of its member tables' pass rates, not the pooled rate. Without "
        "a subject, answer across all tables.\n"
    ),
    (
        "To explain a change in score — in either direction — compare the latest run with the most "
        "recent prior run whose value differs, and rank contributors by their change in failed "
        "tests. A rule with no prior value is a newly added check being evaluated for the first "
        "time, not one that passed before. If no prior run differs, say the score has been stable "
        "over the available history.\n"
    ),
    (
        "There is no target or SLA in this data: report rates and changes without judging them "
        "against a goal, attribute what you can see to the rule, table, column, dimension, or "
        "severity in front of you, and say plainly when a cause (such as an upstream data change) "
        "is outside what you can observe. Row-level failing records are not available here — they "
        "are in the DQX Studio Results tab, where access follows table permissions.\n"
    ),
    (
        "Keep answers short: a paragraph or two, with bullets only for genuine multi-item "
        "breakdowns. Define a term briefly if the steward may not know it. Each sentence should "
        "add something new — a number, a cause, a definition, or a next step — and when there is "
        "nothing more to add, stop.\n"
    ),
]

# id_factory contract shared by the pure builders: mirrors
# ``secrets.token_hex`` (n bytes -> 2n hex chars).
IdFactory = Callable[[int], str]


def _plain_fqn(catalog: str, schema: str, name: str) -> str:
    """Dotted (unquoted) three-part name — the form Genie data-source identifiers use."""
    return f"{catalog}.{schema}.{name}"


def _lines(sql: str) -> list[str]:
    """Split a SQL string into the per-line array the Genie API expects, each
    line keeping its trailing newline except the last (so concatenation
    rebuilds the original query)."""
    raw = list(sql.strip("\n").split("\n"))
    return [ln + "\n" if i < len(raw) - 1 else ln for i, ln in enumerate(raw)]


def _curated_sqls(catalog: str, schema: str) -> list[dict]:
    """Curated (question, SQL) examples — one per pre-canned chip question.

    All aggregate: metric-view questions read ``mv_dq_scores`` with
    MEASURE(); the column-attribution question explodes the mapped
    ``columns`` array on ``v_dq_check_results``. Every question defaults to
    ``run_mode = 'published'`` (drafts only when explicitly asked — the one
    draft question filters ``run_mode = 'draft'``). Table-scoped queries are
    parameterized (:table_name) so they register as trusted assets. Returned
    WITHOUT ids — :func:`build_serialized_space` assigns + sorts.
    """
    mv = quote_object_fqn(catalog, schema, METRIC_VIEW_NAME)
    v = quote_object_fqn(catalog, schema, SHAPING_VIEW_NAME)

    latest_published = (
        "  AND `run_time` = (SELECT MAX(`run_time`) FROM " + mv + "\n"
        "                    WHERE `input_location` = :table_name AND `run_mode` = 'published')"
    )

    current_score = (
        "SELECT MEASURE(`score`) AS pass_rate,\n"
        "       MEASURE(`failed_tests`) AS failed_tests,\n"
        "       MEASURE(`total_tests`) AS total_tests\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        f"{latest_published}"
    )

    failed_in_latest = (
        "SELECT MEASURE(`failed_tests`) AS failed_tests,\n"
        "       MEASURE(`total_tests`) AS total_tests\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        f"{latest_published}"
    )

    rules_failing = (
        "SELECT `check_name`, `dimension`, `severity`,\n"
        "       MEASURE(`failed_tests`) AS failed_tests,\n"
        "       MEASURE(`total_tests`) AS total_tests\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        f"{latest_published}\n"
        "GROUP BY `check_name`, `dimension`, `severity`\n"
        "HAVING MEASURE(`failed_tests`) > 0\n"
        "ORDER BY failed_tests DESC"
    )

    # Column attribution: each check row carries the AS-OF-RUN mapped
    # `columns` array (from the frozen rendered rule set), so failures are
    # attributed to every column the failing check maps to. This is
    # rule-to-column attribution, NOT row-level column failures (the raw
    # rows are not in this space by design).
    columns_most_failures = (
        "SELECT col AS column_name,\n"
        "       SUM(`error_count` + `warning_count`) AS failed_tests,\n"
        "       COUNT(DISTINCT `check_name`) AS failing_rules\n"
        f"FROM {v}\n"
        "LATERAL VIEW explode(`columns`) c AS col\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        "  AND (`error_count` + `warning_count`) > 0\n"
        "  AND `run_time` = (SELECT MAX(`run_time`) FROM " + v + "\n"
        "                    WHERE `input_location` = :table_name AND `run_mode` = 'published')\n"
        "GROUP BY col\n"
        "ORDER BY failed_tests DESC"
    )

    most_severe = (
        "SELECT `severity`, `check_name`, `dimension`,\n"
        "       MEASURE(`failed_tests`) AS failed_tests\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        f"{latest_published}\n"
        "GROUP BY `severity`, `check_name`, `dimension`\n"
        "HAVING MEASURE(`failed_tests`) > 0\n"
        "ORDER BY CASE `severity` WHEN 'Critical' THEN 0 WHEN 'High' THEN 1\n"
        "              WHEN 'Medium' THEN 2 WHEN 'Low' THEN 3 ELSE 4 END,\n"
        "         failed_tests DESC"
    )

    # Latest PUBLISHED run per table, resolved in a CTE so the window runs
    # over the aggregated grid rather than inside the metric view (and a
    # table whose newest run is a draft still surfaces its newest published
    # run).
    lowest_tables = (
        "WITH per_run AS (\n"
        "  SELECT `input_location`, `run_time`,\n"
        "         MEASURE(`score`) AS pass_rate,\n"
        "         MEASURE(`failed_tests`) AS failed_tests\n"
        f"  FROM {mv}\n"
        "  WHERE `run_mode` = 'published'\n"
        "  GROUP BY `input_location`, `run_time`\n"
        ")\n"
        "SELECT `input_location`, pass_rate, failed_tests\n"
        "FROM per_run\n"
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY `input_location` ORDER BY `run_time` DESC) = 1\n"
        "ORDER BY pass_rate ASC\n"
        "LIMIT 20"
    )

    weakest_dims = (
        "SELECT `dimension`,\n"
        "       MEASURE(`score`) AS pass_rate,\n"
        "       MEASURE(`failed_tests`) AS failed_tests\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        f"{latest_published}\n"
        "GROUP BY `dimension`\n"
        "ORDER BY pass_rate ASC"
    )

    score_trend = (
        "SELECT `run_time`, MEASURE(`score`) AS pass_rate\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        "GROUP BY `run_time`\n"
        "ORDER BY `run_time`"
    )

    severity_trend = (
        "SELECT `run_time`, `severity`, MEASURE(`score`) AS pass_rate\n"
        f"FROM {mv}\n"
        "WHERE `input_location` = :table_name\n"
        "  AND `run_mode` = 'published'\n"
        "GROUP BY `run_time`, `severity`\n"
        "ORDER BY `run_time`"
    )

    # Period-over-period decomposition. Polarity-neutral. Reused by the
    # "what's driving / why did it change / biggest factor" diagnose family.
    #
    # Look-back: the two NEWEST runs are very often identical, so a naive
    # newest-vs-second-newest comparison shows no change. Instead `cur` is
    # the latest run and `prev` the most recent PRIOR run whose table-level
    # failed_tests actually DIFFERS, falling back to the immediately-prior
    # run so the grid stays non-empty and Genie can say "no change over the
    # available history". Joins are null-safe (<=>) because `dimension` is
    # NULL for untagged checks.
    diagnose = (
        "WITH run_totals AS (\n"
        "  SELECT `run_time` AS run_ts, MEASURE(`failed_tests`) AS failed_tests\n"
        f"  FROM {mv} WHERE `input_location` = :table_name AND `run_mode` = 'published'\n"
        "  GROUP BY `run_time`\n"
        "),\n"
        "cur_ts AS (SELECT MAX(run_ts) AS run_ts FROM run_totals),\n"
        "cur_total AS (\n"
        "  SELECT failed_tests FROM run_totals WHERE run_ts = (SELECT run_ts FROM cur_ts)\n"
        "),\n"
        "prev_ts AS (\n"
        "  SELECT COALESCE(\n"
        "    (SELECT MAX(run_ts) FROM run_totals\n"
        "     WHERE run_ts < (SELECT run_ts FROM cur_ts)\n"
        "       AND failed_tests <> (SELECT failed_tests FROM cur_total)),\n"
        "    (SELECT MAX(run_ts) FROM run_totals\n"
        "     WHERE run_ts < (SELECT run_ts FROM cur_ts))\n"
        "  ) AS run_ts\n"
        "),\n"
        "cur AS (\n"
        "  SELECT `check_name` AS rule, `dimension` AS dim,\n"
        "         MEASURE(`failed_tests`) AS failed_tests, MEASURE(`total_tests`) AS total_tests\n"
        f"  FROM {mv} WHERE `input_location` = :table_name AND `run_mode` = 'published'\n"
        "    AND `run_time` = (SELECT run_ts FROM cur_ts)\n"
        "  GROUP BY `check_name`, `dimension`\n"
        "),\n"
        "prev AS (\n"
        "  SELECT `check_name` AS rule, `dimension` AS dim,\n"
        "         MEASURE(`failed_tests`) AS failed_tests, MEASURE(`total_tests`) AS total_tests\n"
        f"  FROM {mv} WHERE `input_location` = :table_name AND `run_mode` = 'published'\n"
        "    AND `run_time` = (SELECT run_ts FROM prev_ts)\n"
        "  GROUP BY `check_name`, `dimension`\n"
        ")\n"
        "SELECT COALESCE(c.rule, p.rule) AS rule_name,\n"
        "       COALESCE(c.dim, p.dim) AS dimension,\n"
        "       COALESCE(p.failed_tests, 0) AS prev_failed_tests,\n"
        "       COALESCE(c.failed_tests, 0) AS curr_failed_tests,\n"
        "       COALESCE(c.failed_tests, 0) - COALESCE(p.failed_tests, 0) AS delta_failed_tests,\n"
        "       CASE WHEN COALESCE(p.total_tests, 0) = 0 AND COALESCE(c.total_tests, 0) > 0 THEN 'new rule'\n"
        "            WHEN COALESCE(c.total_tests, 0) > COALESCE(p.total_tests, 0)\n"
        "                 AND COALESCE(c.failed_tests, 0) - COALESCE(p.failed_tests, 0) > 0 THEN 'more data'\n"
        "            ELSE 'fail rate changed' END AS reason\n"
        "FROM cur c FULL OUTER JOIN prev p ON c.rule <=> p.rule AND c.dim <=> p.dim\n"
        "ORDER BY delta_failed_tests DESC"
    )

    dim_diagnose = (
        "WITH run_totals AS (\n"
        "  SELECT `run_time` AS run_ts, MEASURE(`failed_tests`) AS failed_tests\n"
        f"  FROM {mv} WHERE `input_location` = :table_name AND `run_mode` = 'published'\n"
        "  GROUP BY `run_time`\n"
        "),\n"
        "cur_ts AS (SELECT MAX(run_ts) AS run_ts FROM run_totals),\n"
        "cur_total AS (\n"
        "  SELECT failed_tests FROM run_totals WHERE run_ts = (SELECT run_ts FROM cur_ts)\n"
        "),\n"
        "prev_ts AS (\n"
        "  SELECT COALESCE(\n"
        "    (SELECT MAX(run_ts) FROM run_totals\n"
        "     WHERE run_ts < (SELECT run_ts FROM cur_ts)\n"
        "       AND failed_tests <> (SELECT failed_tests FROM cur_total)),\n"
        "    (SELECT MAX(run_ts) FROM run_totals\n"
        "     WHERE run_ts < (SELECT run_ts FROM cur_ts))\n"
        "  ) AS run_ts\n"
        "),\n"
        "cur AS (\n"
        "  SELECT `dimension` AS dim, MEASURE(`score`) AS pass_rate,\n"
        "         MEASURE(`failed_tests`) AS failed_tests\n"
        f"  FROM {mv} WHERE `input_location` = :table_name AND `run_mode` = 'published'\n"
        "    AND `run_time` = (SELECT run_ts FROM cur_ts)\n"
        "  GROUP BY `dimension`\n"
        "),\n"
        "prev AS (\n"
        "  SELECT `dimension` AS dim, MEASURE(`score`) AS pass_rate,\n"
        "         MEASURE(`failed_tests`) AS failed_tests\n"
        f"  FROM {mv} WHERE `input_location` = :table_name AND `run_mode` = 'published'\n"
        "    AND `run_time` = (SELECT run_ts FROM prev_ts)\n"
        "  GROUP BY `dimension`\n"
        ")\n"
        "SELECT COALESCE(c.dim, p.dim) AS dimension,\n"
        "       p.pass_rate AS prev_pass_rate, c.pass_rate AS curr_pass_rate,\n"
        "       COALESCE(c.failed_tests, 0) - COALESCE(p.failed_tests, 0) AS delta_failed_tests\n"
        "FROM cur c FULL OUTER JOIN prev p ON c.dim <=> p.dim\n"
        "ORDER BY ABS(COALESCE(c.failed_tests, 0) - COALESCE(p.failed_tests, 0)) DESC"
    )

    # The one deliberately draft-scoped question — the exception to the
    # published-by-default rule (the question itself names drafts).
    draft_runs = (
        "SELECT COUNT(DISTINCT `run_id`) AS draft_runs_last_7_days\n"
        f"FROM {v}\n"
        "WHERE `run_mode` = 'draft'\n"
        "  AND `run_time` >= current_timestamp() - INTERVAL 7 DAYS"
    )

    table_param = [
        {
            "name": "table_name",
            "description": ["Fully-qualified name of the table to scope to (catalog.schema.table)."],
            "type_hint": "STRING",
        }
    ]

    return [
        {
            "question": ["What is the current data quality score?"],
            "sql": _lines(current_score),
            "parameters": table_param,
            "usage_guidance": [
                "Latest published-run pass rate for one table from mv_dq_scores. "
                "Report the score (pass rate) as the headline; failed/total tests as context."
            ],
        },
        {
            "question": ["How many tests failed in the latest run?"],
            "sql": _lines(failed_in_latest),
            "parameters": table_param,
            "usage_guidance": [
                "failed_tests at the table's latest published run from mv_dq_scores, "
                "with total_tests as the denominator."
            ],
        },
        {
            "question": ["Which rules are failing?"],
            "sql": _lines(rules_failing),
            "parameters": table_param,
            "usage_guidance": [
                "One row per failing rule (check_name) at the table's latest published run; "
                "HAVING keeps only rules with failing tests. dimension/severity may be NULL "
                "for untagged checks."
            ],
        },
        {
            "question": ["Which columns have the most failures?"],
            "sql": _lines(columns_most_failures),
            "parameters": table_param,
            "usage_guidance": [
                "Attribution-based: explodes each failing check's mapped columns array on "
                "v_dq_check_results (latest published run), so failed tests are attributed to "
                "every column the failing rule maps to. This is rule-to-column attribution — "
                "row-level column failures are not available in this space."
            ],
        },
        {
            "question": ["What are my most severe issues right now?"],
            "sql": _lines(most_severe),
            "parameters": table_param,
            "usage_guidance": [
                "Failing rules at the table's latest published run grouped by severity and "
                "ordered Critical -> Low. Lead with the most severe."
            ],
        },
        {
            "question": ["Which tables have the lowest pass rate?"],
            "sql": _lines(lowest_tables),
            "usage_guidance": [
                "Each table's latest published run from mv_dq_scores, ascending by pass rate "
                "(score). Render as a horizontal bar."
            ],
        },
        {
            "question": ["Which quality dimensions are weakest?"],
            "sql": _lines(weakest_dims),
            "parameters": table_param,
            "usage_guidance": [
                "Latest published-run pass rate by quality dimension for the table, lowest "
                "first. A NULL dimension is the untagged bucket."
            ],
        },
        {
            "question": ["How has the score changed over recent runs?"],
            "sql": _lines(score_trend),
            "parameters": table_param,
            "usage_guidance": [
                "Pass-rate trend over published runs for one table. Render as a time-series line."
            ],
        },
        {
            "question": ["How has my DQ score by severity been changing over time?"],
            "sql": _lines(severity_trend),
            "parameters": table_param,
            "usage_guidance": [
                "Pass rate per run_time split by severity for one table (published runs). "
                "One line per severity."
            ],
        },
        {
            "question": ["What is driving my changes in score over time?"],
            "sql": _lines(diagnose),
            "parameters": table_param,
            "usage_guidance": [
                "Period-over-period decomposition (polarity-neutral): compares the latest "
                "published run against the most recent prior run whose table-level failed_tests "
                "differs (the two newest runs are often identical), ranking contributors by the "
                "change in failed_tests. The reason column distinguishes a changed fail rate, a "
                "new rule, and more data at the same rate. Reuse for any 'why did quality "
                "change / biggest factor' question."
            ],
        },
        {
            "question": ["Why did my DQ score change since the last run?"],
            "sql": _lines(diagnose),
            "parameters": table_param,
            "usage_guidance": [
                "Same latest-vs-latest-differing-prior decomposition as 'what is driving my "
                "changes' — state whether the pass rate went up or down and name the top "
                "contributors."
            ],
        },
        {
            "question": ["Why has my score by dimension changed?"],
            "sql": _lines(dim_diagnose),
            "parameters": table_param,
            "usage_guidance": [
                "Pass rate and failed-tests delta per quality dimension across the latest "
                "published run and the most recent prior run that differs, largest move first."
            ],
        },
        {
            "question": ["What is the biggest factor affecting my DQ score?"],
            "sql": _lines(diagnose),
            "parameters": table_param,
            "usage_guidance": [
                "The top contributor from the latest-vs-latest-differing-prior decomposition — "
                "the rule / dimension with the largest change in failed_tests."
            ],
        },
        {
            "question": ["How many draft runs happened recently?"],
            "sql": _lines(draft_runs),
            "usage_guidance": [
                "Distinct draft runs in the last 7 days from v_dq_check_results — the one "
                "question that deliberately filters run_mode = 'draft' (the question names "
                "drafts explicitly)."
            ],
        },
    ]


# Which curated questions to promote to in-Genie benchmarks, with extra
# rephrasings that reuse the exact same SQL. The remaining curated questions
# become "stretch" benchmarks (one phrasing each). Row-level flagships from
# dqlake are dropped with the row-level questions themselves.
_BENCHMARK_CORE = {
    "Which columns have the most failures?": [
        "Which column has the most failing tests?",
    ],
    "What are my most severe issues right now?": [
        "Show my most critical data-quality issues.",
    ],
    "What is driving my changes in score over time?": [
        "Why did my DQ score change since the last run?",
        "What is the biggest factor affecting my DQ score?",
    ],
    "Which rules are failing?": [
        "Which data-quality rules are failing right now?",
    ],
}


def _benchmarks(curated: list[dict]) -> list[dict]:
    """Benchmark question+SQL pairs.

    Core: the flagship example-SQL questions plus rephrasings, all reusing the
    already-validated curated SQL verbatim so ground truth never drifts.
    Stretch: every remaining curated question, once. Returned WITHOUT ids —
    :func:`build_serialized_space` assigns + sorts them.
    """
    by_q = {c["question"][0]: c for c in curated}
    out: list[dict] = []
    seen: set[str] = set()

    for flagship, rephrasings in _BENCHMARK_CORE.items():
        sql = list(by_q[flagship]["sql"])
        for q in [flagship, *rephrasings]:
            if q in seen:
                continue
            seen.add(q)
            out.append({"question": [q], "answer": [{"format": "SQL", "content": sql}]})

    for c in curated:
        q = c["question"][0]
        if q in seen:
            continue
        seen.add(q)
        out.append({"question": [q], "answer": [{"format": "SQL", "content": list(c["sql"])}]})
    return out


def _column_configs(catalog: str, schema: str) -> dict[str, list[dict]]:
    """Per-table column_configs enabling prompt matching (format assistance +
    entity matching) on the string filter columns users name by value.

    Built via the API, prompt matching is OFF by default, so we set it
    explicitly. Keyed by table identifier; each list is sorted by column_name
    (the API requirement). Entity matching requires format assistance, so both
    are set together."""

    def cc(name: str, description: str) -> dict:
        return {
            "column_name": name,
            "description": [description],
            "enable_format_assistance": True,
            "enable_entity_matching": True,
        }

    results = _plain_fqn(catalog, schema, SHAPING_VIEW_NAME)
    attribution = _plain_fqn(catalog, schema, ATTRIBUTION_VIEW_NAME)
    return {
        results: sorted(
            [
                cc("check_name", "Name of the data-quality rule (check) that was evaluated."),
                cc(
                    "criticality",
                    "DQX criticality the check ran with: 'error' or 'warn'. Internal framing — prefer severity in answers.",
                ),
                cc("dimension", "Quality dimension of the check (Completeness, Validity, ...). NULL when untagged."),
                cc("input_location", "Fully-qualified name (catalog.schema.table) of the monitored SOURCE table."),
                cc("run_mode", "Run provenance: 'published' or 'draft'. Default to published."),
                cc("severity", "Severity of the check: Critical, High, Medium, or Low. NULL when untagged."),
            ],
            key=lambda c: c["column_name"],
        ),
        attribution: sorted(
            [
                cc("check_name", "Name of the data-quality rule (check) in the run's frozen rule set."),
                cc("dimension", "Quality dimension tag frozen into the rule at run time."),
                cc("severity", "Severity tag frozen into the rule at run time: Critical, High, Medium, or Low."),
                cc("source_table_fqn", "Fully-qualified name (catalog.schema.table) of the monitored SOURCE table."),
            ],
            key=lambda c: c["column_name"],
        ),
    }


def _sql_snippets(catalog: str, schema: str) -> dict:
    """Space-native SQL expressions (measures / filters / expressions), all
    table-qualified per the schema. Returned WITHOUT ids —
    :func:`build_serialized_space` assigns + sorts them."""
    mv = _plain_fqn(catalog, schema, METRIC_VIEW_NAME)
    measures = [
        {
            "alias": "pass_rate",
            "display_name": "Pass Rate",
            "sql": [f"MEASURE(`{mv}`.`score`)"],
            "synonyms": ["quality score", "data quality score", "score"],
            "instruction": [
                "Share of tests that passed (0-1) at the test grain. Always read via MEASURE(); "
                "never average across runs/tables."
            ],
        },
        {
            "alias": "failed_tests",
            "display_name": "Failed Tests",
            "sql": [f"MEASURE(`{mv}`.`failed_tests`)"],
            "synonyms": ["failures", "failed test count", "number of failures"],
            "instruction": [
                "Count of record-level tests that failed (errors + warnings). Rank "
                "period-over-period changes by the change in this."
            ],
        },
    ]
    filters = [
        {
            "display_name": "published runs",
            "sql": [f"`{mv}`.`run_mode` = 'published'"],
            "synonyms": ["published only", "official runs", "excluding drafts"],
            "instruction": [
                "Apply by DEFAULT to every question — draft runs only when the question "
                "explicitly asks about drafts."
            ],
        },
    ]
    expressions = [
        {
            "alias": "severity_rank",
            "display_name": "Severity Rank",
            "sql": [
                "CASE `" + mv + "`.`severity` WHEN 'Critical' THEN 0 "
                "WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 WHEN 'Low' THEN 3 ELSE 4 END"
            ],
            "synonyms": ["severity order", "most severe first"],
            "instruction": [
                "Order severities Critical, High, Medium, Low (most to least severe) when "
                "ranking failing rules by severity."
            ],
        },
    ]
    return {"measures": measures, "filters": filters, "expressions": expressions}


def _attach_ids(snippets: dict, id_factory: IdFactory) -> dict:
    """Assign 32-hex ids to each snippet and sort each list by id."""
    return {
        kind: sorted([{"id": id_factory(16), **s} for s in items], key=lambda x: x["id"])
        for kind, items in snippets.items()
    }


def build_serialized_space(catalog: str, schema: str, *, id_factory: IdFactory = secrets.token_hex) -> dict:
    """Build the full serialized_space v2 tree over the app's score objects."""
    mv = _plain_fqn(catalog, schema, METRIC_VIEW_NAME)
    results = _plain_fqn(catalog, schema, SHAPING_VIEW_NAME)
    attribution = _plain_fqn(catalog, schema, ATTRIBUTION_VIEW_NAME)
    column_configs = _column_configs(catalog, schema)
    # Each attached object carries a description grounded in its REAL columns
    # so Genie routes questions correctly. Aggregates only — no raw-row
    # objects (see the module docstring).
    table_sources = sorted(
        [
            {
                "identifier": results,
                "description": [
                    "Per-check DQ results — one row per (run_id, input_location, check_name) "
                    "across all runs. Columns: run_id, input_location (the monitored table's "
                    "fully-qualified name), run_time, is_latest_run, check_name, error_count, "
                    "warning_count, input_row_count (tests evaluated for the check), run_mode "
                    "('published' | 'draft' — default to published), binding_version, and the "
                    "AS-OF-RUN attribution the check executed with: criticality, severity "
                    "(Critical/High/Medium/Low), dimension (quality dimension), "
                    "registry_rule_id, and columns (ARRAY of the column names the check maps "
                    "to — explode it for column-level failure attribution). failed tests for a "
                    "check = error_count + warning_count. Attribution columns are NULL for "
                    "untagged or legacy runs. Prefer mv_dq_scores for rates and trends."
                ],
                "column_configs": column_configs[results],
            },
            {
                "identifier": attribution,
                "description": [
                    "As-of-the-run rule attribution — one row per (run_id, source_table_fqn, "
                    "check_name), parsed from the run's frozen rendered rule set. Columns: "
                    "run_id, source_table_fqn, check_name, criticality, severity, dimension, "
                    "registry_rule_id, columns (ARRAY of mapped column names). Use for rule-set "
                    "questions (which checks ran, what they were tagged with, which columns "
                    "they map to); it carries no pass/fail counts."
                ],
                "column_configs": column_configs[attribution],
            },
        ],
        key=lambda x: x["identifier"],
    )
    question_entries = sorted(
        [{"id": id_factory(16), "question": [q]} for q in SAMPLE_QUESTIONS],
        key=lambda x: x["id"],
    )
    curated = _curated_sqls(catalog, schema)
    example_sqls = sorted([{"id": id_factory(16), **e} for e in curated], key=lambda x: x["id"])
    benchmark_qs = sorted([{"id": id_factory(16), **b} for b in _benchmarks(curated)], key=lambda x: x["id"])
    return {
        "version": 2,
        "config": {"sample_questions": question_entries},
        "data_sources": {
            "tables": table_sources,
            "metric_views": [
                {
                    "identifier": mv,
                    "description": [
                        "DQ score metric view: measures score (pass rate, 0-1), failed_tests, "
                        "and total_tests over dimensions input_location, run_id, run_time, "
                        "is_latest_run, run_mode, check_name, severity, dimension, criticality. "
                        "Read measures with MEASURE(); group by run_time for trends; filter "
                        "run_mode = 'published' by default."
                    ],
                }
            ],
        },
        "instructions": {
            "text_instructions": [{"id": id_factory(16), "content": list(TEXT_INSTRUCTIONS)}],
            "example_question_sqls": example_sqls,
            "sql_snippets": _attach_ids(_sql_snippets(catalog, schema), id_factory),
            "join_specs": [],
            "sql_functions": [],
        },
        "benchmarks": {"questions": benchmark_qs},
    }


def build_create_payload(
    catalog: str,
    schema: str,
    *,
    warehouse_id: str,
    parent_path: str,
    id_factory: IdFactory = secrets.token_hex,
) -> dict:
    """Build the POST /api/2.0/genie/spaces body."""
    return {
        "serialized_space": json.dumps(build_serialized_space(catalog, schema, id_factory=id_factory)),
        "warehouse_id": warehouse_id,
        "parent_path": parent_path,
        "title": SPACE_TITLE,
        "description": SPACE_DESCRIPTION,
    }


def _deterministic_id_factory() -> IdFactory:
    """A counter-based id_factory so :func:`build_serialized_space` produces
    identical ids (and therefore identical id-sorted ordering) on every call —
    required for a stable config hash. NOT used for real provisioning (which
    wants random ids); only for hashing."""
    counter = {"n": 0}

    def f(_n: int = 16) -> str:
        counter["n"] += 1
        return f"{counter['n']:032x}"

    return f


def config_hash(catalog: str, schema: str) -> str:
    """Stable sha256 of the serialized space's content for the given objects.

    Built with a deterministic id_factory so the random per-build ids (and the
    id-keyed sort order they drive) don't perturb the hash; the resulting tree
    is dumped to canonical JSON (sorted keys, no whitespace) and hashed. Two
    builds of the same content always hash equal, so startup can tell whether
    the space config has genuinely changed since it was last provisioned.
    """
    content = build_serialized_space(catalog, schema, id_factory=_deterministic_id_factory())
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _find_space_id_by_title(ws: WorkspaceClient, title: str) -> str | None:
    """Find an existing space to REUSE (so we don't recreate one per boot).

    Databricks appends a timestamp to the space title on create — e.g.
    "DQX Studio — DQ Results 2026-06-18 12:05:16" — so an exact-title match
    never hits and a fresh space would get created every boot (leaving
    orphaned duplicates). Match by PREFIX instead and return the
    most-recently-created match (the timestamp suffix sorts
    lexicographically). Page through the list so a match is not missed when
    the workspace has many spaces.
    """
    try:
        matches: list[dict] = []
        page_token: str | None = None
        for _ in range(20):  # cap paging defensively
            query: dict = {"page_size": 100}
            if page_token:
                query["page_token"] = page_token
            resp = ws.api_client.do("GET", "/api/2.0/genie/spaces", query=query)
            spaces = resp.get("spaces") if isinstance(resp, dict) else None
            for sp in spaces or []:
                t = sp.get("title") or ""
                if t == title or t.startswith(title):
                    matches.append(sp)
            page_token = resp.get("next_page_token") if isinstance(resp, dict) else None
            if not page_token:
                break
        if matches:
            matches.sort(key=lambda sp: sp.get("title") or "", reverse=True)
            return matches[0].get("space_id")
    except Exception as e:
        # Best-effort resilience contract: listing spaces is an optimisation
        # (reuse instead of create). Any workspace API failure here must
        # degrade to "not found" so provisioning can still proceed/skip.
        logger.info(f"Genie space list skipped: {e}")
    return None


def _update_serialized_space(ws: WorkspaceClient, space_id: str, catalog: str, schema: str) -> bool:
    """PATCH the existing space's serialized_space with the freshly-built config.

    The Genie REST API supports updating a space via
    PATCH /api/2.0/genie/spaces/{space_id} with a {"serialized_space": "..."}
    body. Returns True on success.
    """
    body = {"serialized_space": json.dumps(build_serialized_space(catalog, schema))}
    try:
        ws.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
        return True
    except Exception as e:
        # Best-effort resilience contract: a failed PATCH must never break
        # startup — the existing space still answers with its old config and
        # the un-persisted hash makes the next startup retry.
        logger.warning(f"Genie space update skipped: {type(e).__name__}: {e}")
        return False


def ensure_dq_genie_space(
    *,
    settings: AppSettingsService,
    ws: WorkspaceClient,
    warehouse_id: str,
    parent_path: str,
    catalog: str,
    schema: str,
) -> str | None:
    """Idempotent, self-healing provision of the DQ Genie space (SP identity).

    Behaviour, keyed on the ``dq_genie_space_id`` +
    ``dq_genie_space_config_hash`` settings:

    - no space id              -> find-or-create (POST), store id + hash, status ready
    - id present, hash same    -> no-op (return id, leave status as-is)
    - id present, hash changed -> update the space (PATCH serialized_space);
      on success store the new hash; on failure keep the OLD hash so the
      next startup sees a mismatch and RETRIES the update (persisting the
      new hash on failure would silently swallow the config change forever
      after one transient flap), and leave the space usable (status ready).

    Best-effort: returns the space_id or None and never raises out of the
    app lifespan. Maintains ``dq_genie_space_status``
    (provisioning|ready|error).
    """
    try:
        existing = settings.get_setting(SETTING_SPACE_ID)
        stored_hash = settings.get_setting(SETTING_CONFIG_HASH)
        desired_hash = config_hash(catalog, schema)

        # Already-provisioned and unchanged: cheap no-op.
        if existing and stored_hash == desired_hash:
            return existing

        # Already-provisioned but the config drifted: update in place.
        if existing:
            settings.save_setting(SETTING_STATUS, STATUS_PROVISIONING)
            updated = _update_serialized_space(ws, existing, catalog, schema)
            if updated:
                settings.save_setting(SETTING_CONFIG_HASH, desired_hash)
                settings.save_setting(SETTING_STATUS, STATUS_READY)
            else:
                # The PATCH failed (usually a transient flap). Do NOT persist
                # the new hash: leaving stored_hash at its OLD value means the
                # next provision sees a mismatch and RETRIES the update. The
                # space still exists and answers, so keep it usable — never
                # delete a space we can't cleanly recreate.
                settings.save_setting(SETTING_STATUS, STATUS_READY)
                logger.warning(
                    f"Genie space update failed; left existing space {existing} "
                    "in place — will retry on next provision"
                )
            return existing

        # No id stored: find-or-create.
        settings.save_setting(SETTING_STATUS, STATUS_PROVISIONING)
        space_id = _find_space_id_by_title(ws, SPACE_TITLE)
        if space_id is None:
            payload = build_create_payload(catalog, schema, warehouse_id=warehouse_id, parent_path=parent_path)
            try:
                resp = ws.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
                space_id = resp.get("space_id") if isinstance(resp, dict) else None
            except Exception as e:
                # Best-effort resilience contract: space creation failing
                # (permissions, API availability) must degrade to "no Genie"
                # rather than a crash-looping app.
                logger.warning(f"Genie space create skipped: {type(e).__name__}: {e}")
                settings.save_setting(SETTING_STATUS, STATUS_ERROR)
                return None

        if space_id:
            settings.save_setting(SETTING_SPACE_ID, space_id)
            settings.save_setting(SETTING_CONFIG_HASH, desired_hash)
            settings.save_setting(SETTING_STATUS, STATUS_READY)
        else:
            settings.save_setting(SETTING_STATUS, STATUS_ERROR)
        return space_id
    except Exception:
        # Best-effort resilience contract: never raise out of the app
        # lifespan — Genie is an optional feature, not a startup dependency.
        logger.exception("Genie space ensure failed")
        try:
            settings.save_setting(SETTING_STATUS, STATUS_ERROR)
        except Exception:
            # Even the status write can fail (settings store down); the
            # feature simply stays unavailable until the next startup.
            logger.warning("Could not record Genie space error status", exc_info=True)
        return None
