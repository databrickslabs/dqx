"""Dataset loaders for the conditioning harness.

Every real dataset is downloaded at run time and cached under ``~/.cache/dqx-benchmarks``. Nothing
is vendored into this repository, which is what keeps the licensing position simple: DQX
redistributes none of it. Citations and licences are recorded in the harness README and reproduced
in any published results table.

SMAP and MSL are deliberately absent. Their data files carry "(c) Original Authors" with no
permissive licence, so they cannot be used even under download-at-runtime.
"""
