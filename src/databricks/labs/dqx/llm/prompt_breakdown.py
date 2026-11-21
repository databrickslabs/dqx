"""LLM prompt-size observability for AI-assisted rule generation.

Two concerns live here:

- :class:`PromptSource` / :class:`PromptBreakdown` — the per-source size
  breakdown callers log at INFO on every rule-generation call for
  budget-tracking. Observability only; no cap enforcement.

- :func:`measure_schema_prompt` — stateless function that parses the schema
  JSON (typically from
  ``UnityCatalogMetadata.json()``) and produces a *PromptBreakdown*.

Kept import-cheap on purpose: no DSPy, no PySpark — only standard library.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)


PromptSourceKind = Literal[
    "user_input",
    "available_functions",
    "summary_stats",
    "source_table",
    "upstream_table",
    "external_lineage",
    "column_lineage_edges",
]


@dataclass(frozen=True)
class PromptSource:
    """One contribution to the final LLM prompt, measured in characters.

    Attributes:
        kind: Which part of the prompt this entry describes. Table-scoped kinds
            (*source_table*, *upstream_table*) carry a canonical 3-part UC name;
            *external_lineage* carries the upstream source name as reported by the
            Unity Catalog external-lineage API. Scalar kinds (*user_input*,
            *available_functions*, *summary_stats*, *column_lineage_edges*) leave
            *name* as *None*.
        chars: Number of characters this source contributes to the serialised prompt.
        name: Identifier of the source — a 3-part UC table name or an external
            source name — or *None* for scalar kinds.
        depth: Hop distance from the seed table for *upstream_table* entries;
            *None* otherwise.
        detail: Optional sub-field breakdown, e.g.
            ``{"columns": 4120, "table_tags": 310, "comment": 55}`` for a table block.
            *None* when no sub-fields are tracked.
    """

    kind: PromptSourceKind
    chars: int
    name: str | None = None
    depth: int | None = None
    detail: dict[str, int] | None = None


@dataclass(frozen=True)
class PromptBreakdown:
    """Per-source breakdown of the final LLM prompt.

    Instances are immutable; use :meth:`merge` to combine two breakdowns (the engine
    merges schema-side sources produced by :func:`measure_schema_prompt` with its own
    engine-side entries for *user_input*, *available_functions*, and *summary_stats*).

    Attributes:
        total_chars: Sum of ``chars`` across all *sources*. Maintained as a separate
            attribute so callers do not re-scan the tuple.
        sources: Ordered tuple of :class:`PromptSource` entries describing the
            prompt. Order is informational only; :meth:`render` sorts by *chars*
            before emitting.
    """

    total_chars: int
    sources: tuple[PromptSource, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        computed = sum(s.chars for s in self.sources)
        if computed != self.total_chars:
            raise ValueError(
                f"PromptBreakdown invariant violated: total_chars={self.total_chars} "
                f"but sum of source chars={computed}"
            )

    def merge(self, other: "PromptBreakdown") -> "PromptBreakdown":
        """Return a new :class:`PromptBreakdown` with concatenated sources and summed totals.

        Preserves order (*self* entries first, then *other*); callers that want
        determinism should impose their own ordering before calling :meth:`render`.
        """
        return PromptBreakdown(
            total_chars=self.total_chars + other.total_chars,
            sources=self.sources + other.sources,
        )

    def render(self) -> str:
        """Return a multi-line table describing each source, sorted by *chars* desc.

        Columns: *kind | name | depth | chars | % of total*. A trailing TOTAL row gives
        the aggregate character count. Designed for INFO-level logging — the output is
        a single string with embedded newlines so a single
        ``logger.info(breakdown.render())`` call keeps the whole report together.
        """
        header = ("kind", "name", "depth", "chars", "% of total")
        rows: list[tuple[str, str, str, str, str]] = []
        ordered = sorted(self.sources, key=lambda s: s.chars, reverse=True)
        total = self.total_chars or 1  # avoid /0 in degenerate cases; 0 sources means 0% everywhere anyway
        for source in ordered:
            pct = f"{(source.chars / total) * 100:.1f}%"
            name_cell = source.name or "-"
            depth_cell = "-" if source.depth is None else str(source.depth)
            rows.append((source.kind, name_cell, depth_cell, f"{source.chars:,}", pct))
        rows.append(("TOTAL", "-", "-", f"{self.total_chars:,}", "100.0%"))

        widths = tuple(max(len(h), *(len(row[i]) for row in rows)) for i, h in enumerate(header))
        lines = [" | ".join(h.ljust(widths[i]) for i, h in enumerate(header))]
        lines.append("-+-".join("-" * widths[i] for i in range(len(header))))
        for row in rows:
            lines.append(" | ".join(row[i].ljust(widths[i]) for i in range(len(header))))
        return "LLM prompt size breakdown:\n" + "\n".join(lines)


def _collect_upstream_sources(payload: dict) -> tuple[list[PromptSource], int]:
    sources: list[PromptSource] = []
    accounted = 0
    for entry in payload.get("upstream_tables", []) or []:
        if not isinstance(entry, dict):
            continue
        detail = {k: len(json.dumps(v)) for k, v in entry.items() if k not in {"name", "depth"}}
        chars = len(json.dumps(entry))
        accounted += chars
        sources.append(
            PromptSource(
                kind="upstream_table",
                chars=chars,
                name=entry.get("name"),
                depth=entry.get("depth"),
                detail=detail or None,
            )
        )
    return sources, accounted


def _collect_external_lineage_sources(payload: dict) -> tuple[list[PromptSource], int]:
    sources: list[PromptSource] = []
    accounted = 0
    for entry in payload.get("external_lineage", []) or []:
        if not isinstance(entry, dict):
            continue
        chars = len(json.dumps(entry))
        accounted += chars
        source_name = entry.get("source_name") or None
        source_kind: Any = entry.get("source_kind")
        lineage_detail = {"source_kind": len(json.dumps(source_kind))} if source_kind else None
        sources.append(
            PromptSource(
                kind="external_lineage",
                chars=chars,
                name=source_name,
                detail=lineage_detail,
            )
        )
    return sources, accounted


def _build_seed_detail(payload: dict) -> dict[str, int]:
    seed_detail: dict[str, int] = {"columns": len(json.dumps(payload.get("columns", [])))}
    if "table_comment" in payload:
        seed_detail["table_comment"] = len(json.dumps(payload["table_comment"]))
    if "table_tags" in payload:
        seed_detail["table_tags"] = len(json.dumps(payload["table_tags"]))
    return seed_detail


def measure_schema_prompt(schema_json: str) -> PromptBreakdown:
    """Parse *schema_json* and emit one :class:`PromptBreakdown` describing its size shape.

    Produces one *source_table* entry for the seed (name = parsed ``"table"`` when
    present), one *upstream_table* entry per ``upstream_tables[]`` item (with
    ``(name, depth)`` and a sub-field detail dict), one *external_lineage* entry per
    ``external_lineage[]`` item, and an aggregate *column_lineage_edges* entry when
    ``column_upstream_lineage[]`` is non-empty. The seed *source_table* entry absorbs the
    remaining bytes (``len(schema_json) - sum(other sources)``) so the
    :class:`PromptBreakdown` invariant ``sum(chars) == total_chars`` holds trivially.

    Robust to malformed input: returns a single opaque *source_table* entry covering the
    whole string if *schema_json* is not valid JSON or not an object. The engine must
    never crash a rule-generation call because of a logging helper.
    """
    total = len(schema_json)
    try:
        payload = json.loads(schema_json) if schema_json else None
    except json.JSONDecodeError as exc:
        logger.warning(f"measure_schema_prompt: non-JSON schema_info ({exc}); emitting opaque source_table entry")
        payload = None

    if not isinstance(payload, dict):
        return PromptBreakdown(
            total_chars=total,
            sources=(PromptSource(kind="source_table", chars=total),),
        )

    upstream_sources, upstream_accounted = _collect_upstream_sources(payload)
    external_sources, external_accounted = _collect_external_lineage_sources(payload)
    sources: list[PromptSource] = upstream_sources + external_sources
    accounted = upstream_accounted + external_accounted

    edges = payload.get("column_upstream_lineage")
    if edges:
        chars = len(json.dumps(edges))
        accounted += chars
        sources.append(
            PromptSource(
                kind="column_lineage_edges",
                chars=chars,
                detail={"edge_count": len(edges)},
            )
        )

    seed_chars = total - accounted
    sources.insert(
        0,
        PromptSource(
            kind="source_table",
            chars=seed_chars,
            name=payload.get("table"),
            detail=_build_seed_detail(payload),
        ),
    )

    return PromptBreakdown(total_chars=total, sources=tuple(sources))


__all__ = [
    "PromptSource",
    "PromptSourceKind",
    "PromptBreakdown",
    "measure_schema_prompt",
]
