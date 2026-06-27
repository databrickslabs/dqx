"""Generate app.yml from command-line KEY=VALUE and KEY_FROM=RESOURCE pairs.

Called by the DABs build step so env vars are baked into app.yml before sync.
DABs substitutes ${var.xxx} in the build command before the shell runs, so
each argument arrives here with its literal resolved value.

KEY=VALUE      → env:  - name: KEY / value: VALUE
KEY_FROM=RES   → env:  - name: KEY / value_from: RES
"""

import pathlib
import sys

COMMAND = [
    "uvicorn",
    "databricks_labs_dqx_app.backend.app:app",
    "--workers",
    "2",
]

env_lines: list[str] = []
for arg in sys.argv[1:]:
    k, v = arg.split("=", 1)
    if k.endswith("_FROM"):
        name = k[:-5]
        env_lines.append(f"- name: {name}\n  value_from: {v}")
    else:
        env_lines.append(f"- name: {k}\n  value: {v}")

command_yaml = "\n".join(f"- {c}" for c in COMMAND)
env_yaml = ("\n".join(env_lines)) if env_lines else ""

content = f"command:\n{command_yaml}\n"
if env_yaml:
    content += f"env:\n{env_yaml}\n"

pathlib.Path("app.yml").write_text(content)
print("Generated app.yml")
