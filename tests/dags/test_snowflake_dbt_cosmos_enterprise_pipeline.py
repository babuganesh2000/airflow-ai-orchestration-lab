"""Tests for the enterprise Snowflake + dbt + Cosmos DAG."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path

# Astro's one-off pytest container does not initialize the Airflow metadata DB.
# Disable Cosmos' Airflow Variable-backed dbt ls cache before Airflow/Cosmos
# modules can read configuration.
os.environ.setdefault("AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS", "False")

import pytest
from airflow.models import DagBag


DAG_ID = "snowflake_dbt_cosmos_enterprise_pipeline"
DAG_FILE = (
    Path(__file__).parent.parent.parent
    / "dags"
    / "snowflake_dbt_cosmos_enterprise_pipeline.py"
)
EXPECTED_CONN_ID = "snowflake_default"


@contextmanager
def suppress_logging(namespace: str):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    with suppress_logging("airflow"):
        return DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)


@pytest.fixture(scope="module")
def enterprise_dag(dag_bag: DagBag):
    return dag_bag.dags.get(DAG_ID)


def test_dag_file_exists():
    assert DAG_FILE.exists(), f"DAG file not found: {DAG_FILE}"


def test_no_import_errors(dag_bag: DagBag):
    errors = {
        path: err
        for path, err in dag_bag.import_errors.items()
        if "snowflake_dbt_cosmos_enterprise_pipeline" in path
    }
    assert not errors, f"Import errors in enterprise DAG:\n{errors}"


def test_dag_registered(enterprise_dag):
    assert enterprise_dag is not None, f"{DAG_ID} was not registered"


def test_enterprise_dag_configuration(enterprise_dag):
    if enterprise_dag is None:
        pytest.skip("DAG not loaded")

    assert enterprise_dag.catchup is False
    assert enterprise_dag.default_args.get("retries") >= 2
    assert {"enterprise", "snowflake", "dbt", "cosmos", "claims"}.issubset(
        set(enterprise_dag.tags)
    )


def test_expected_task_structure(enterprise_dag):
    if enterprise_dag is None:
        pytest.skip("DAG not loaded")

    task_ids = {task.task_id for task in enterprise_dag.tasks}
    assert "generate_local_synthetic_batches" in task_ids
    assert "initialize_snowflake_raw_objects" in task_ids
    assert "load_batch_001_to_raw" in task_ids
    assert "load_batch_002_to_raw" in task_ids
    assert "load_batch_003_to_raw" in task_ids
    assert "complete_batch_001_audit" in task_ids
    assert "complete_batch_002_audit" in task_ids
    assert "complete_batch_003_audit" in task_ids
    for batch_suffix in ["001", "002", "003"]:
        assert any(
            f"dbt_after_batch_{batch_suffix}" in task_id
            for task_id in task_ids
        )


def test_sql_tasks_use_snowflake_default_conn(enterprise_dag):
    if enterprise_dag is None:
        pytest.skip("DAG not loaded")

    offenders = [
        task.task_id
        for task in enterprise_dag.tasks
        if getattr(task, "conn_id", None) not in (None, EXPECTED_CONN_ID)
    ]
    assert not offenders, (
        f"Tasks using a conn_id other than {EXPECTED_CONN_ID}: {offenders}"
    )


def test_batch_ids_drive_idempotent_raw_loads(enterprise_dag):
    if enterprise_dag is None:
        pytest.skip("DAG not loaded")

    for batch_id in ["batch_001", "batch_002", "batch_003"]:
        task = enterprise_dag.get_task(
            f"load_{batch_id.replace('batch_', 'batch_')}_to_raw"
        )
        load_sql = "\n".join(task.sql)
        assert batch_id in load_sql
        assert f"DELETE FROM AF_CLAIMS_ANALYTICS.RAW.CLAIMS_RAW WHERE batch_id = '{batch_id}'" in load_sql
        assert f"@AF_CLAIMS_ANALYTICS.RAW.{batch_id.upper()}_STAGE" in load_sql
        assert "COPY INTO AF_CLAIMS_ANALYTICS.RAW.CLAIMS_RAW" in load_sql


def test_each_batch_load_runs_before_its_dbt_group_and_audit(enterprise_dag):
    if enterprise_dag is None:
        pytest.skip("DAG not loaded")

    initialize_task = enterprise_dag.get_task("initialize_snowflake_raw_objects")
    load_batch_001 = enterprise_dag.get_task("load_batch_001_to_raw")
    load_batch_002 = enterprise_dag.get_task("load_batch_002_to_raw")
    load_batch_003 = enterprise_dag.get_task("load_batch_003_to_raw")
    complete_batch_001 = enterprise_dag.get_task("complete_batch_001_audit")
    complete_batch_002 = enterprise_dag.get_task("complete_batch_002_audit")
    complete_batch_003 = enterprise_dag.get_task("complete_batch_003_audit")

    assert "generate_local_synthetic_batches" in initialize_task.upstream_task_ids
    assert "initialize_snowflake_raw_objects" in load_batch_001.upstream_task_ids
    assert any(
        task_id.startswith("dbt_after_batch_001")
        for task_id in complete_batch_001.upstream_task_ids
    )
    assert "complete_batch_001_audit" in load_batch_002.upstream_task_ids
    assert any(
        task_id.startswith("dbt_after_batch_002")
        for task_id in complete_batch_002.upstream_task_ids
    )
    assert "complete_batch_002_audit" in load_batch_003.upstream_task_ids
    assert any(
        task_id.startswith("dbt_after_batch_003")
        for task_id in complete_batch_003.upstream_task_ids
    )
