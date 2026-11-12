"""In-process caches that keep repeated rule-policy evaluations cheap.

Opening a monitored table (and listing its suggestions) evaluates every rule
policy reaching it. Three process-wide structures keep that from re-reading
Lakebase and Unity Catalog on every open:

- :class:`PolicyGeneration` — a counter bumped by anything that can change what a
  policy evaluation produces (a rule policy saved, deleted or re-scanned, a rule
  approved, edited or deleted, a table registered). Every cache entry below is
  tied to the generation it was made in, so a user's own edit takes effect on the
  next read.
- :class:`PolicyCatalogCache` — the policy list and their rules for one generation
  (with a short TTL as a safety net for changes made by another process).
- :class:`OpenEvaluationDebounce` — remembers which ``(user, binding)`` was
  evaluated on open in this generation within the last few minutes, so reopening
  the table schedules nothing.

The app runs a single worker process, so in-process state is shared by every
request. All structures are thread-safe and bounded.
"""

import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from databricks_labs_dqx_app.backend.policy_models import RulePolicy
from databricks_labs_dqx_app.backend.registry_models import RegistryRule

CATALOG_TTL_SECONDS = 60.0
OPEN_DEBOUNCE_SECONDS = 300.0
OPEN_RUNNING_TIMEOUT_SECONDS = 120.0  # a claimed evaluation that never finished is retried after this
MAX_DEBOUNCE_ENTRIES = 10_000


class PolicyGeneration:
    """Monotonic counter of changes that can alter a rule policy evaluation."""

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def current(self) -> int:
        with self._lock:
            return self._value

    def bump(self) -> int:
        with self._lock:
            self._value += 1
            return self._value


POLICY_GENERATION = PolicyGeneration()


def bump_policy_generation() -> None:
    """Record a change that can alter rule policy evaluations (invalidates the caches below)."""
    POLICY_GENERATION.bump()


@dataclass
class _CatalogEntry:
    generation: int
    expires_at: float
    policies: list[RulePolicy] | None = None
    rules: dict[str, RegistryRule | None] | None = None


class PolicyCatalogCache:
    """Every rule policy and the rules they belong to, cached per generation."""

    def __init__(
        self,
        generation: PolicyGeneration = POLICY_GENERATION,
        *,
        ttl_seconds: float = CATALOG_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._generation = generation
        self._ttl = ttl_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._entry: _CatalogEntry | None = None

    def _current(self, generation: int) -> _CatalogEntry:
        entry = self._entry
        if entry is None or entry.generation != generation or self._clock() >= entry.expires_at:
            entry = _CatalogEntry(generation=generation, expires_at=self._clock() + self._ttl)
            self._entry = entry
        return entry

    def policies(self, load: Callable[[], list[RulePolicy]]) -> list[RulePolicy]:
        """Every rule policy; *load* runs only when this generation has none cached."""
        generation = self._generation.current()
        with self._lock:
            cached = self._current(generation).policies
        if cached is not None:
            return list(cached)
        loaded = load()  # read outside the lock; a bump meanwhile makes the store below stale on the next read
        with self._lock:
            entry = self._current(generation)
            if entry.generation == generation:
                entry.policies = list(loaded)
        return list(loaded)

    def rules(
        self, rule_ids: list[str], load: Callable[[list[str]], dict[str, RegistryRule]]
    ) -> dict[str, RegistryRule]:
        """The rules among *rule_ids* that exist; *load* reads only the ones not cached in this generation."""
        generation = self._generation.current()
        with self._lock:
            known = dict(self._current(generation).rules or {})
        missing = [rule_id for rule_id in rule_ids if rule_id not in known]
        if missing:
            loaded = load(missing)
            fetched: dict[str, RegistryRule | None] = {rule_id: loaded.get(rule_id) for rule_id in missing}
            known.update(fetched)
            with self._lock:
                entry = self._current(generation)
                if entry.generation == generation:
                    entry.rules = {**(entry.rules or {}), **fetched}
        return {rule_id: rule for rule_id in rule_ids if (rule := known.get(rule_id)) is not None}


class OpenClaim(Enum):
    """What a table open should do about policy evaluation."""

    RUN = "run"  # evaluate now (in the background); call :meth:`OpenEvaluationDebounce.finish` when done
    PENDING = "pending"  # an evaluation claimed earlier is still running
    SKIP = "skip"  # evaluated recently in this generation


@dataclass
class _OpenEntry:
    claimed_at: float
    running: bool


class OpenEvaluationDebounce:
    """Debounces on-open policy evaluation per ``(user, binding)`` and generation."""

    def __init__(
        self,
        generation: PolicyGeneration = POLICY_GENERATION,
        *,
        ttl_seconds: float = OPEN_DEBOUNCE_SECONDS,
        running_timeout_seconds: float = OPEN_RUNNING_TIMEOUT_SECONDS,
        max_entries: int = MAX_DEBOUNCE_ENTRIES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._generation = generation
        self._ttl = ttl_seconds
        self._running_timeout = running_timeout_seconds
        self._max_entries = max_entries
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[str, str, int], _OpenEntry] = OrderedDict()

    def claim(self, user_key: str, binding_id: str) -> tuple[OpenClaim, tuple[str, str, int]]:
        """Decide whether an open of *binding_id* by *user_key* evaluates; returns the claim and its key."""
        key = (user_key, binding_id, self._generation.current())
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                if entry.running and now - entry.claimed_at < self._running_timeout:
                    return OpenClaim.PENDING, key
                if not entry.running and now - entry.claimed_at < self._ttl:
                    return OpenClaim.SKIP, key
            self._entries[key] = _OpenEntry(claimed_at=now, running=True)
            self._entries.move_to_end(key)
            self._evict(now)
        return OpenClaim.RUN, key

    def finish(self, key: tuple[str, str, int]) -> None:
        """Mark the evaluation claimed under *key* as done; reopening within the TTL then skips."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                entry.running = False

    def _evict(self, now: float) -> None:
        horizon = max(self._ttl, self._running_timeout)
        expired = [k for k, e in self._entries.items() if now - e.claimed_at >= horizon]
        for k in expired:
            del self._entries[k]
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


POLICY_CATALOG_CACHE = PolicyCatalogCache()
OPEN_EVALUATION_DEBOUNCE = OpenEvaluationDebounce()
