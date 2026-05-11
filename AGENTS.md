# Airflow + Snowflake + dbt + Cosmos Engineering Rules

You are working in an Astro Airflow project.

Architecture rules:
- Airflow orchestrates only.
- Snowflake performs warehouse execution.
- dbt owns transformations and tests.
- Cosmos renders dbt models as Airflow tasks.
- Never hardcode credentials.
- Use Airflow connections, especially conn_id="snowflake_default".
- Do not make Snowflake calls at DAG parse time.
- Keep XCom small. Return metadata only.
- DAGs must be idempotent by batch_id or logical date.
- Use retries only for transient failures.
- Add audit logging for enterprise pipelines.
- Use mode="reschedule" for long-running sensors.
- Add DAG import tests.
- Prefer SQLExecuteQueryOperator for Snowflake SQL.
- Prefer DbtTaskGroup for dbt execution through Cosmos.
- Do not put giant transformation SQL inside Airflow DAGs.