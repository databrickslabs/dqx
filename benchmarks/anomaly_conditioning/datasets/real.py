"""Real datasets with genuine categorical groupings, downloaded at run time.

Both are used because they fail differently. SMD's grouping (server entity) explains a great deal of
the variance and is the case conditioning is meant for. NSL-KDD's groupings (network service and
protocol) are the awkward case: hundreds of services, many tiny, which is where per-group models
become both expensive and badly calibrated.

ADBench is deliberately not used despite being the obvious choice: it ships pre-processed ``.npz``
numeric matrices, so categorical column identity is gone and there is no group left to condition
on. Only raw datasets retain what this experiment measures.

Licences, for the results page:

* **SMD** (Server Machine Dataset) — MIT, from ``NetManAIOps/OmniAnomaly``. Cite Su et al., KDD
  2019, "Robust Anomaly Detection for Multivariate Time Series through Stochastic Recurrent Neural
  Networks".
* **NSL-KDD** — redistributable with citation. Cite Tavallaee et al., CISDA 2009, "A detailed
  analysis of the KDD CUP 99 data set".

Nothing is committed to this repository; files are cached under ``~/.cache/dqx-benchmarks``.
"""

import pathlib
import urllib.request
from collections.abc import Iterator

import numpy as np

CACHE = pathlib.Path.home() / ".cache" / "dqx-benchmarks"

SMD_BASE = "https://raw.githubusercontent.com/NetManAIOps/OmniAnomaly/master/ServerMachineDataset"
# 28 entities: verified against the repository, where machine-3-12 is a 404.
SMD_ENTITIES = (
    [f"machine-1-{i}" for i in range(1, 9)]
    + [f"machine-2-{i}" for i in range(1, 10)]
    + [f"machine-3-{i}" for i in range(1, 12)]
)
NSLKDD_URL = "https://raw.githubusercontent.com/defcom17/NSL_KDD/master/KDDTrain%2B.txt"

# Rows per SMD entity. The full test set is ~708k rows over 38 features, and the sweep fits every
# configuration five times; capping keeps a run to minutes without changing the comparison, since
# every configuration sees exactly the same rows.
SMD_MAX_ROWS_PER_ENTITY = 4000

# NSL-KDD is ~46% attacks, which is a classification problem rather than an anomaly-detection one.
# Attacks are downsampled to this rate so the task matches what DQX actually does, and so PR-AUC
# means what it means everywhere else in this harness.
NSLKDD_ANOMALY_RATE = 0.02


def _fetch(url: str, name: str) -> pathlib.Path:
    """Download *url* into the cache once, and return the local path."""
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / name
    if path.exists() and path.stat().st_size > 0:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=120) as resp:  # noqa: S310 - fixed https literals above
        path.write_bytes(resp.read())
    return path


def _load_smd() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Concatenate every entity's labelled test split.

    SMD ships train (unlabelled) and test (labelled) per entity. Only the test split carries labels,
    so that is what is measured; a configuration is fitted and scored on it, exactly as in the
    synthetic sweep, which keeps the two comparable.
    """
    values_list, labels_list, entity_list = [], [], []
    for entity in SMD_ENTITIES:
        test = np.loadtxt(_fetch(f"{SMD_BASE}/test/{entity}.txt", f"smd/test-{entity}.txt"), delimiter=",")
        labels = np.loadtxt(_fetch(f"{SMD_BASE}/test_label/{entity}.txt", f"smd/label-{entity}.txt"), delimiter=",")
        take = min(len(test), len(labels), SMD_MAX_ROWS_PER_ENTITY)
        values_list.append(test[:take])
        labels_list.append(labels[:take])
        entity_list.append(np.full(take, entity))
    return np.vstack(values_list), np.concatenate(labels_list), np.concatenate(entity_list)


def _load_nslkdd() -> tuple[np.ndarray, np.ndarray, dict[str, np.ndarray]]:
    """Return ``(numeric values, labels, {grouping name: group labels})``.

    Columns 1-3 are ``protocol_type``, ``service`` and ``flag``; column 41 is the attack name, where
    ``normal`` is the only non-attack value. Everything else numeric becomes a feature.
    """
    path = _fetch(NSLKDD_URL, "nslkdd/KDDTrain+.txt")
    rows = [line.split(",") for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    categorical_idx = {1: "protocol_type", 2: "service", 3: "flag"}
    label_idx = 41
    numeric_idx = [i for i in range(len(rows[0])) if i not in categorical_idx and i not in (label_idx, label_idx + 1)]

    values = np.array([[float(row[i]) for i in numeric_idx] for row in rows])
    labels = np.array([0.0 if row[label_idx] == "normal" else 1.0 for row in rows])
    groupings = {name: np.array([row[idx] for row in rows]) for idx, name in categorical_idx.items()}

    # Downsample attacks to a realistic anomaly rate. Seeded so the frame is identical across
    # configurations and seeds -- the forest seed varies, the data does not.
    rng = np.random.default_rng(0)
    normal_idx = np.flatnonzero(labels == 0)
    attack_idx = np.flatnonzero(labels == 1)
    n_keep = max(1, int(len(normal_idx) * NSLKDD_ANOMALY_RATE / (1 - NSLKDD_ANOMALY_RATE)))
    keep = np.sort(np.concatenate([normal_idx, rng.choice(attack_idx, min(n_keep, len(attack_idx)), replace=False)]))

    return values[keep], labels[keep], {name: groups[keep] for name, groups in groupings.items()}


def load_groupings(name: str) -> Iterator[tuple[str, np.ndarray, np.ndarray, np.ndarray]]:
    """Yield ``(grouping label, values, labels, groups)`` for each candidate grouping of *name*."""
    if name == "smd":
        values, labels, entities = _load_smd()
        yield "entity", values, labels, entities
        # The machine family prefix: a coarser grouping over the same data, which is the kind of
        # choice a user actually faces. Included so the sweep has a within-dataset contrast between
        # a fine and a coarse grouping rather than one point per dataset.
        yield "machine_family", values, labels, np.array([e.rsplit("-", 1)[0] for e in entities])
        return

    if name == "nslkdd":
        values, labels, groupings = _load_nslkdd()
        for grouping_name, groups in groupings.items():
            yield grouping_name, values, labels, groups
        return

    raise ValueError(f"unknown dataset {name!r}")
