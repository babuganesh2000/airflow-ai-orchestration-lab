Create or update an enterprise-grade Airflow DAG in this Astro project.

Goal:
Build a Snowflake + dbt + Cosmos pipeline.

Requirements:
1. Use conn_id="snowflake_default".
2. Use SQLExecuteQueryOperator for Snowflake setup/load tasks.
3. Use Cosmos DbtTaskGroup for dbt transformations.
4. DAG name: snowflake_dbt_cosmos_enterprise_pipeline.
5. Load sample healthcare claims data into RAW.CLAIMS.
6. dbt should transform RAW.CLAIMS into MART models.
7. Add idempotency using {{ ds_nodash }} as BATCH_ID.
8. Add tests under tests/.
9. Do not hardcode passwords.
10. Do not query Snowflake at DAG parse time.
11. Keep XCom small.
12. Follow AGENTS.md strictly.

After editing:
- Show changed files.
- Run or suggest:
  - astro dev parse
  - pytest