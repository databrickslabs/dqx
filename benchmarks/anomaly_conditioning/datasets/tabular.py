"""Classical tabular anomaly benchmarks, for the regime that has no grouping at all.

These answer a different question from the rest of the harness. SMD and NSL-KDD exist to test whether
conditioning on a group helps; these exist to characterise how DQX's mechanism performs on the
bread-and-butter case the field actually benchmarks on, and to confirm that adding baseline
conditioning did not disturb it.

Source: [ADBench](https://github.com/Minqi824/ADBench) (BSD-2-Clause), which redistributes the ODDS /
UCI / Kaggle collections as `.npz` matrices of `X` and `y`. Downloaded at run time and cached; never
vendored.

An earlier draft of this harness dismissed ADBench outright because its pre-processing discards
categorical column identity. That is a real limitation, but only for the grouping question — a
plain-tabular regime does not need a grouping, so excluding it here was too broad a call.

The selection spans three axes deliberately, because a single dataset tells you almost nothing about
a detector:

* **Scale**: 1.8k rows (cardio) to 285k (fraud).
* **Dimensionality**: 9 features (shuttle) to 100 (mnist).
* **Anomaly rate**: 0.17% (fraud) to ~32% (campaign-adjacent), which matters enormously — PR-AUC is
  not comparable across base rates, so each dataset is reported against its own base rate and its own
  trivial baselines rather than pooled into one headline number.

`13_fraud` is the Kaggle credit-card competition set; `10_cover` is Covertype; `32_shuttle`,
`30_satellite`, `23_mammography`, `38_thyroid` and `6_cardio` are the long-standing ODDS benchmarks
most papers report.
"""

import io
import pathlib
import urllib.request
from collections.abc import Iterator

import numpy as np

CACHE = pathlib.Path.home() / ".cache" / "dqx-benchmarks" / "adbench"
BASE = "https://raw.githubusercontent.com/Minqi824/ADBench/main/adbench/datasets/Classical"

# name -> ADBench file. Kept explicit rather than globbed so a run is reproducible and a new upstream
# dataset cannot silently change the published table.
DATASETS = {
    "cardio": "6_cardio.npz",
    "thyroid": "38_thyroid.npz",
    "mammography": "23_mammography.npz",
    "satellite": "30_satellite.npz",
    "shuttle": "32_shuttle.npz",
    "covertype": "10_cover.npz",
    "spambase": "35_SpamBase.npz",
    "campaign": "5_campaign.npz",
    "mnist": "24_mnist.npz",
    "fraud": "13_fraud.npz",
}

# Cap for the largest sets so a full sweep stays in minutes. Stratified so the anomaly rate -- the
# thing PR-AUC is most sensitive to -- is preserved rather than resampled away.
MAX_ROWS = 30000


def _fetch(filename: str) -> pathlib.Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / filename
    if path.exists() and path.stat().st_size > 0:
        return path
    with urllib.request.urlopen(f"{BASE}/{filename}", timeout=300) as resp:  # noqa: S310 - fixed https literal
        path.write_bytes(resp.read())
    return path


def _stratified_cap(values: np.ndarray, labels: np.ndarray, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Cap row count while preserving the anomaly rate."""
    if len(values) <= MAX_ROWS:
        return values, labels
    rng = np.random.default_rng(seed)
    keep_fraction = MAX_ROWS / len(values)
    keep = []
    for label in (0.0, 1.0):
        idx = np.flatnonzero(labels == label)
        n = max(1, int(round(len(idx) * keep_fraction)))
        keep.append(rng.choice(idx, size=min(n, len(idx)), replace=False))
    selected = np.sort(np.concatenate(keep))
    return values[selected], labels[selected]


def load(name: str, *, seed: int = 0) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(values, labels)`` for one dataset."""
    if name not in DATASETS:
        raise ValueError(f"unknown tabular dataset {name!r}; known: {sorted(DATASETS)}")
    with np.load(io.BytesIO(_fetch(DATASETS[name]).read_bytes())) as data:
        values = np.asarray(data["X"], dtype=float)
        labels = np.asarray(data["y"], dtype=float).ravel()
    return _stratified_cap(values, labels, seed)


def iter_datasets(names: list[str] | None = None) -> Iterator[tuple[str, np.ndarray, np.ndarray]]:
    """Yield ``(name, values, labels)`` for the requested datasets, or all of them."""
    for name in names or sorted(DATASETS):
        values, labels = load(name)
        yield name, values, labels
