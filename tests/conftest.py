"""Shared pytest configuration for DAG import tests."""

from __future__ import annotations

import os


# Astro's one-off pytest container does not initialize the Airflow metadata DB.
# Disable Cosmos' Airflow Variable-backed dbt ls cache before test modules
# construct DagBag instances.
os.environ.setdefault("AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS", "False")
