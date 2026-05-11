"""
## Enterprise Claims Analytics Orchestration

This DAG mirrors the local dbt/Snowflake claims analytics project:

1. Generate deterministic local CSV files for batches ``batch_001``,
   ``batch_002``, and ``batch_003``.
2. Create Snowflake raw, staging, intermediate, mart, snapshot, and audit
   objects.
3. Load each local batch into Snowflake raw tables through internal stages.
4. Run dbt transformations through Cosmos ``DbtTaskGroup`` after each batch.
5. Record enterprise batch audit metadata after each batch is transformed.

Airflow orchestrates only. Snowflake executes warehouse work, dbt owns
transformations and tests, and Cosmos renders dbt resources as Airflow tasks.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pendulum import datetime


SNOWFLAKE_CONN_ID = "snowflake_default"
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_PATH = PROJECT_ROOT / "include" / "dbt"
SYNTHETIC_BATCH_SCRIPT = PROJECT_ROOT / "scripts" / "generate_synthetic_batches.py"

DATABASE = "AF_CLAIMS_ANALYTICS"
RAW_SCHEMA = "RAW"
AUDIT_SCHEMA = "AUDIT"
AIRFLOW_PROJECT_ROOT = "/usr/local/airflow"

BATCHES = ("batch_001", "batch_002", "batch_003")

REFERENCE_ENTITIES = {
    "facilities": "FACILITIES_RAW",
    "payers": "PAYERS_RAW",
    "collectors": "COLLECTORS_RAW",
}

BATCH_ENTITIES = {
    "accounts": "ACCOUNTS_RAW",
    "claims": "CLAIMS_RAW",
    "remittances": "REMITTANCES_RAW",
    "workqueue_actions": "WORKQUEUE_ACTIONS_RAW",
}

PROFILE_CONFIG = ProfileConfig(
    profile_name="claims_analytics",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "database": DATABASE,
            "schema": "STG",
        },
    ),
)


SETUP_SQL = [
    f"CREATE DATABASE IF NOT EXISTS {DATABASE}",
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.STG",
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.INT",
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.MART",
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.SNAPSHOT",
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{AUDIT_SCHEMA}",
    f"""
    CREATE FILE FORMAT IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.CSV_FF
      TYPE = CSV
      SKIP_HEADER = 1
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      NULL_IF = ('', 'NULL', 'null')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{AUDIT_SCHEMA}.PIPELINE_BATCH_LOG (
        batch_id       VARCHAR      NOT NULL,
        pipeline_name  VARCHAR      NOT NULL,
        dag_id         VARCHAR      NOT NULL,
        run_id         VARCHAR      NOT NULL,
        status         VARCHAR      NOT NULL,
        rows_loaded    NUMBER(38,0) DEFAULT 0,
        started_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
        completed_at   TIMESTAMP_NTZ,
        error_message   VARCHAR,
        PRIMARY KEY (batch_id, pipeline_name)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.ACCOUNTS_RAW (
        account_id NUMBER(38,0),
        patient_id NUMBER(38,0),
        facility_id NUMBER(38,0),
        current_balance NUMBER(18,2),
        assignment_status VARCHAR,
        collector_id NUMBER(38,0),
        queue_name VARCHAR,
        updated_at TIMESTAMP_NTZ,
        load_ts TIMESTAMP_NTZ,
        batch_id VARCHAR,
        src_file_name VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.CLAIMS_RAW (
        claim_id NUMBER(38,0),
        account_id NUMBER(38,0),
        facility_id NUMBER(38,0),
        payer_id NUMBER(38,0),
        service_date DATE,
        billed_amount NUMBER(18,2),
        claim_status VARCHAR,
        load_ts TIMESTAMP_NTZ,
        batch_id VARCHAR,
        src_file_name VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.REMITTANCES_RAW (
        remit_id NUMBER(38,0),
        claim_id NUMBER(38,0),
        payment_date DATE,
        paid_amount NUMBER(18,2),
        adjustment_amount NUMBER(18,2),
        carc_code VARCHAR,
        rarc_code VARCHAR,
        remit_status VARCHAR,
        load_ts TIMESTAMP_NTZ,
        batch_id VARCHAR,
        src_file_name VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.WORKQUEUE_ACTIONS_RAW (
        action_id NUMBER(38,0),
        account_id NUMBER(38,0),
        collector_id NUMBER(38,0),
        action_date DATE,
        action_type VARCHAR,
        note_count NUMBER(38,0),
        worked_flag VARCHAR,
        load_ts TIMESTAMP_NTZ,
        batch_id VARCHAR,
        src_file_name VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.FACILITIES_RAW (
        facility_id NUMBER(38,0),
        facility_name VARCHAR,
        state VARCHAR,
        region VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.PAYERS_RAW (
        payer_id NUMBER(38,0),
        payer_name VARCHAR,
        payer_category VARCHAR,
        contract_type VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.COLLECTORS_RAW (
        collector_id NUMBER(38,0),
        collector_name VARCHAR,
        manager_name VARCHAR,
        region VARCHAR,
        active_flag VARCHAR
    )
    """,
    *[
        f"CREATE STAGE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.{batch.upper()}_STAGE "
        f"FILE_FORMAT = {DATABASE}.{RAW_SCHEMA}.CSV_FF"
        for batch in BATCHES
    ],
]


def _put_sql(batch_id: str, entity: str) -> str:
    return (
        f"PUT file://{AIRFLOW_PROJECT_ROOT}/data/{batch_id}/{entity}.csv "
        f"@{DATABASE}.{RAW_SCHEMA}.{batch_id.upper()}_STAGE "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )


def _copy_sql(batch_id: str, entity: str, table_name: str) -> str:
    return f"""
    COPY INTO {DATABASE}.{RAW_SCHEMA}.{table_name}
    FROM @{DATABASE}.{RAW_SCHEMA}.{batch_id.upper()}_STAGE
    PATTERN = '.*{entity}[.]csv'
    FILE_FORMAT = (FORMAT_NAME = {DATABASE}.{RAW_SCHEMA}.CSV_FF)
    FORCE = TRUE
    ON_ERROR = 'ABORT_STATEMENT'
    """


def _batch_load_sql(batch_id: str) -> list[str]:
    stage_name = f"{DATABASE}.{RAW_SCHEMA}.{batch_id.upper()}_STAGE"
    sql = [
        f"REMOVE @{stage_name}",
        f"""
        MERGE INTO {DATABASE}.{AUDIT_SCHEMA}.PIPELINE_BATCH_LOG AS target
        USING (
            SELECT
                '{batch_id}' AS batch_id,
                'claims_analytics' AS pipeline_name,
                '{{{{ dag.dag_id }}}}' AS dag_id,
                '{{{{ run_id }}}}' AS run_id
        ) AS source
        ON target.batch_id = source.batch_id
        AND target.pipeline_name = source.pipeline_name
        WHEN MATCHED THEN UPDATE SET
            status = 'RUNNING',
            run_id = source.run_id,
            started_at = CURRENT_TIMESTAMP,
            completed_at = NULL,
            error_message = NULL
        WHEN NOT MATCHED THEN INSERT (
            batch_id,
            pipeline_name,
            dag_id,
            run_id,
            status,
            started_at
        ) VALUES (
            source.batch_id,
            source.pipeline_name,
            source.dag_id,
            source.run_id,
            'RUNNING',
            CURRENT_TIMESTAMP
        )
        """,
    ]

    entities = dict(BATCH_ENTITIES)
    if batch_id == "batch_001":
        entities = {**REFERENCE_ENTITIES, **entities}
        sql.extend(
            [
                f"TRUNCATE TABLE {DATABASE}.{RAW_SCHEMA}.FACILITIES_RAW",
                f"TRUNCATE TABLE {DATABASE}.{RAW_SCHEMA}.PAYERS_RAW",
                f"TRUNCATE TABLE {DATABASE}.{RAW_SCHEMA}.COLLECTORS_RAW",
            ]
        )

    for entity in entities:
        sql.append(_put_sql(batch_id, entity))

    for table_name in BATCH_ENTITIES.values():
        sql.append(
            f"DELETE FROM {DATABASE}.{RAW_SCHEMA}.{table_name} "
            f"WHERE batch_id = '{batch_id}'"
        )

    for entity, table_name in entities.items():
        sql.append(_copy_sql(batch_id, entity, table_name))

    sql.append(
        f"""
        UPDATE {DATABASE}.{AUDIT_SCHEMA}.PIPELINE_BATCH_LOG
           SET rows_loaded = (
               SELECT COUNT(*) FROM {DATABASE}.{RAW_SCHEMA}.ACCOUNTS_RAW
                WHERE batch_id = '{batch_id}'
           ) + (
               SELECT COUNT(*) FROM {DATABASE}.{RAW_SCHEMA}.CLAIMS_RAW
                WHERE batch_id = '{batch_id}'
           ) + (
               SELECT COUNT(*) FROM {DATABASE}.{RAW_SCHEMA}.REMITTANCES_RAW
                WHERE batch_id = '{batch_id}'
           ) + (
               SELECT COUNT(*) FROM {DATABASE}.{RAW_SCHEMA}.WORKQUEUE_ACTIONS_RAW
                WHERE batch_id = '{batch_id}'
           ),
               status = 'RAW_LOADED'
         WHERE batch_id = '{batch_id}'
           AND pipeline_name = 'claims_analytics'
        """
    )
    return sql


def _complete_batch_sql(batch_id: str) -> str:
    return f"""
UPDATE {DATABASE}.{AUDIT_SCHEMA}.PIPELINE_BATCH_LOG
   SET status = 'COMPLETED',
       completed_at = CURRENT_TIMESTAMP
 WHERE batch_id = '{batch_id}'
   AND pipeline_name = 'claims_analytics'
"""


def _dbt_task_group(batch_id: str) -> DbtTaskGroup:
    batch_suffix = batch_id.replace("batch_", "")
    return DbtTaskGroup(
        group_id=f"dbt_after_batch_{batch_suffix}",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            install_dbt_deps=False,
        ),
        profile_config=PROFILE_CONFIG,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models"],
        ),
        operator_args={
            "pool": "snowflake_pool",
            "vars": {"batch_id": batch_id},
        },
    )


@dag(
    dag_id="snowflake_dbt_cosmos_enterprise_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["enterprise", "snowflake", "dbt", "cosmos", "claims"],
    doc_md=__doc__,
)
def snowflake_dbt_cosmos_enterprise_pipeline() -> None:
    @task(task_id="generate_local_synthetic_batches", do_xcom_push=False)
    def generate_local_synthetic_batches() -> None:
        subprocess.run(
            [sys.executable, str(SYNTHETIC_BATCH_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            check=True,
        )

    generate_batches = generate_local_synthetic_batches()

    initialize_snowflake = SQLExecuteQueryOperator(
        task_id="initialize_snowflake_raw_objects",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SETUP_SQL,
        split_statements=False,
        pool="snowflake_pool",
    )

    load_batch_001 = SQLExecuteQueryOperator(
        task_id="load_batch_001_to_raw",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_batch_load_sql("batch_001"),
        split_statements=False,
        pool="snowflake_pool",
    )

    load_batch_002 = SQLExecuteQueryOperator(
        task_id="load_batch_002_to_raw",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_batch_load_sql("batch_002"),
        split_statements=False,
        pool="snowflake_pool",
    )

    load_batch_003 = SQLExecuteQueryOperator(
        task_id="load_batch_003_to_raw",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_batch_load_sql("batch_003"),
        split_statements=False,
        pool="snowflake_pool",
    )

    dbt_after_batch_001 = _dbt_task_group("batch_001")
    dbt_after_batch_002 = _dbt_task_group("batch_002")
    dbt_after_batch_003 = _dbt_task_group("batch_003")

    complete_batch_001 = SQLExecuteQueryOperator(
        task_id="complete_batch_001_audit",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_complete_batch_sql("batch_001"),
        pool="snowflake_pool",
    )

    complete_batch_002 = SQLExecuteQueryOperator(
        task_id="complete_batch_002_audit",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_complete_batch_sql("batch_002"),
        pool="snowflake_pool",
    )

    complete_batch_003 = SQLExecuteQueryOperator(
        task_id="complete_batch_003_audit",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_complete_batch_sql("batch_003"),
        pool="snowflake_pool",
    )

    generate_batches >> initialize_snowflake
    initialize_snowflake >> load_batch_001 >> dbt_after_batch_001 >> complete_batch_001
    complete_batch_001 >> load_batch_002 >> dbt_after_batch_002 >> complete_batch_002
    complete_batch_002 >> load_batch_003 >> dbt_after_batch_003 >> complete_batch_003


snowflake_dbt_cosmos_enterprise_pipeline()
