Read AGENTS.md.

Create or update the Astro Airflow project with:
- Snowflake provider
- dbt-core
- dbt-snowflake
- astronomer-cosmos
- A DAG using SQLExecuteQueryOperator and DbtTaskGroup
- dbt models for RAW.CLAIMS to MART summary
- tests for DAG import
- no hardcoded credentials
- no top-level database calls
- idempotent batch_id logic