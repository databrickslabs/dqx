# ==============================
# DQX Installation via Databricks CLI
# ==============================

# --- Prerequisites ---
# Python 3.10 or later
# Databricks workspace access
# Network access to your Databricks workspace
# Databricks CLI v0.241 or later
# (Windows only) Build Tools for Visual Studio

# --- Step 1: Authenticate Databricks CLI ---
databricks auth login --host <WORKSPACE_HOST>

# --- Step 2: Install DQX in your workspace ---
databricks labs install dqx

# Or install a specific version:
databricks labs install dqx@v0.8.0

# --- (Optional) Force installation to a specific location ---
# Global install:
DQX_FORCE_INSTALL=global databricks labs install dqx
# User install (default):
DQX_FORCE_INSTALL=user databricks labs install dqx
# Custom path install:
databricks labs install dqx --path /Shared/dqx-team

# --- Step 3: Open the configuration file after install ---
databricks labs dqx open-remote-config
