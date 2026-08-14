"""Regenerate the suggester-eval replay cache.

Run from ``app/``::

    uv run --group test python -m tests.suggester_eval_recorder

Writes ``tests/fixtures/suggester_eval/recorded/embeddings.json``.

How it works, and why it works this way
---------------------------------------
The recorder does not know how the suggester builds its query texts, and that
is the point. It runs the **real** pipeline with the embedding cache in
fill-on-miss mode, so every text that crosses the ``serving_endpoints.query``
boundary is captured exactly as the suggester produced it. Nothing here
restates a format string from ``rule_suggester.py`` or ``rule_embeddings.py``,
so the fixtures cannot drift from the code — if a query text changes, the next
recorder run captures the new one and Tier 1 raises
:class:`MissingRecordingError` until it is re-run.

Embedding backend
-----------------
Vectors come from :func:`lexical_vector`, a deterministic local stand-in: a
hashed bag-of-tokens with sub-linear term weighting. It is genuinely useful for
ranking (a rule describing "email address" scores above one describing
"currency code" for an ``email`` column) but it is **lexical, not semantic** —
it will not connect ``created_at`` to the word "timestamp" the way
``databricks-gte-large-en`` does.

That is a deliberate Phase-1 tradeoff, and it is why Tier 1 is a
change-detector rather than a quality measurement: it makes the eval runnable
in CI today, with no workspace and no tokens. Capturing vectors from the real
endpoint is Tier 2's job (``tests/ai_eval/``), and when those recordings land
they overwrite this same file — the replay path does not change, only the
numbers in it.

The judge oracle in ``recorded/judge.json`` is authored by hand, not generated
here. See ``suggester_eval_support`` for what that oracle is and is not.
"""

import asyncio
import json
import math
import re
from hashlib import blake2b

from tests.suggester_eval_support import (
    RECORDED,
    EvalTable,
    Recordings,
    build_suggester,
    load_corpus,
    load_tables,
)

# Wide enough that unrelated texts do not collide into spurious similarity,
# narrow enough that the checked-in JSON stays readable and small.
VECTOR_DIM = 96

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> list[str]:
    """Split *text* into comparable tokens.

    Snake_case and dotted identifiers are split by the character class, so
    ``customer_email`` yields ``customer`` and ``email`` and therefore overlaps
    with a rule described in prose as "the email column". Single characters are
    dropped as noise.
    """
    return [token for token in _TOKEN_RE.findall(text.lower()) if len(token) > 1]


def _bucket(token: str) -> int:
    return int.from_bytes(blake2b(token.encode("utf-8"), digest_size=4).digest(), "big") % VECTOR_DIM


def lexical_vector(text: str) -> list[float]:
    """Deterministic hashed bag-of-tokens vector for *text*.

    Term frequency is damped with ``1 + log(count)`` so a word repeated five
    times does not swamp five distinct words, and the vector is L2-normalised
    so cosine similarity is a plain dot product. Deterministic across machines
    and Python versions: blake2b, not the salted builtin ``hash``.
    """
    counts: dict[int, int] = {}
    for token in _tokens(text):
        bucket = _bucket(token)
        counts[bucket] = counts.get(bucket, 0) + 1
    vector = [0.0] * VECTOR_DIM
    for bucket, count in counts.items():
        vector[bucket] = 1.0 + math.log(count)
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0.0:
        return vector
    return [round(value / norm, 6) for value in vector]


async def _drive(tables: list[EvalTable], recordings: Recordings) -> None:
    """Run the real suggester over every fixture table so its texts get captured."""
    corpus = load_corpus()
    for table in tables:
        suggester, _ = build_suggester(table, corpus, recordings)
        result = await suggester.suggest(table.binding_id, "recorder@example.com")
        if not result.available:
            raise SystemExit(f"Suggester degraded while recording {table.table_fqn}: {result.reason}")
        print(f"  {table.table_fqn}: {len(result.suggestions)} suggestion(s)")


def main() -> None:
    tables = load_tables()
    judge = json.loads((RECORDED / "judge.json").read_text(encoding="utf-8"))
    recordings = Recordings(embeddings={}, judge=judge, embedder=lexical_vector)

    print(f"Recording embeddings for {len(tables)} table(s) with the lexical stand-in embedder")
    asyncio.run(_drive(tables, recordings))

    target = RECORDED / "embeddings.json"
    target.write_text(_dump_one_vector_per_line(recordings.embeddings), encoding="utf-8")
    print(f"Wrote {len(recordings.embeddings)} vector(s) to {target}")


def _dump_one_vector_per_line(embeddings: dict[str, list[float]]) -> str:
    """Serialise with each vector on a single line.

    ``json.dumps(indent=...)`` puts every float on its own line, which turns a
    couple of hundred vectors into an eighteen-thousand-line diff nobody can
    review. One line per key keeps the file greppable by digest and keeps a
    re-record to a diff you can actually read.
    """
    lines = [f'  "{digest}": {json.dumps(vector)}' for digest, vector in sorted(embeddings.items())]
    return "{\n" + ",\n".join(lines) + "\n}\n"


if __name__ == "__main__":
    main()
