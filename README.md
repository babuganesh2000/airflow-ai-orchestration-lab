Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Enterprise Claims Analytics Flow
================================

The DAG `snowflake_dbt_cosmos_enterprise_pipeline` mirrors the local
`dbt-snowflake-enterprise-claims-analytics` project:

1. Generate deterministic local CSV batches under `data/batch_001`,
   `data/batch_002`, and `data/batch_003`.
2. Create Snowflake database/schema/raw/audit objects in `AF_CLAIMS_ANALYTICS`.
3. Upload each batch to internal Snowflake stages with `PUT`.
4. Load raw tables with idempotent batch deletes plus `COPY INTO`.
5. Run the dbt analytics graph in `include/dbt` through Cosmos `DbtTaskGroup`.

Configure the Airflow connection `snowflake_default` in `airflow_settings.yaml`
or your secrets backend. The DAG and Cosmos profile mapping both use that
connection; no Snowflake credentials are hardcoded in DAG or dbt files.

Useful local checks:

```bash
python scripts/generate_synthetic_batches.py
python scripts/validate_synthetic_batches.py
astro dev parse
astro dev pytest
```

Claims Mart Streamlit Dashboard
===============================

The Streamlit app in `streamlit_app.py` visualizes the dbt mart tables in
`AF_CLAIMS_ANALYTICS.MART`, including AR balances, claim status, denial
buckets, remittance activity, and collector productivity.

Configure Snowflake access with environment variables:

```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=AF_CLAIMS_ANALYTICS
SNOWFLAKE_SCHEMA=MART
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

Or copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and fill
in local-only credentials. Do not commit real secrets.

Run the dashboard:

```bash
streamlit run streamlit_app.py
```

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
