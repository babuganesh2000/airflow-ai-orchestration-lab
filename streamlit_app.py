from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st


DEFAULT_DATABASE = "AF_CLAIMS_ANALYTICS"
DEFAULT_SCHEMA = "MART"


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str = DEFAULT_DATABASE
    schema: str = DEFAULT_SCHEMA
    role: str | None = None


def _secret(name: str, default: str | None = None) -> str | None:
    if "snowflake" in st.secrets and name.lower() in st.secrets["snowflake"]:
        return st.secrets["snowflake"][name.lower()]
    return os.getenv(f"SNOWFLAKE_{name.upper()}", default)


def get_config() -> SnowflakeConfig:
    missing = []
    values: dict[str, Any] = {}
    for field in ["account", "user", "password", "warehouse"]:
        value = _secret(field)
        if not value:
            missing.append(field)
        values[field] = value

    if missing:
        st.error(
            "Missing Snowflake settings: "
            + ", ".join(f"SNOWFLAKE_{name.upper()}" for name in missing)
            + ". Add them as environment variables or in .streamlit/secrets.toml."
        )
        st.stop()

    values["database"] = _secret("database", DEFAULT_DATABASE)
    values["schema"] = _secret("schema", DEFAULT_SCHEMA)
    values["role"] = _secret("role")
    return SnowflakeConfig(**values)


@st.cache_resource(show_spinner=False)
def get_connection(config: SnowflakeConfig):
    connect_args: dict[str, Any] = {
        "account": config.account,
        "user": config.user,
        "password": config.password,
        "warehouse": config.warehouse,
        "database": config.database,
        "schema": config.schema,
    }
    if config.role:
        connect_args["role"] = config.role
    return snowflake.connector.connect(**connect_args)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str, config: SnowflakeConfig) -> pd.DataFrame:
    conn = get_connection(config)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetch_pandas_all()


def relation(config: SnowflakeConfig, name: str) -> str:
    return f"{config.database}.{config.schema}.{name}"


def metric_card(label: str, value: Any, help_text: str | None = None) -> None:
    st.metric(label, value if value is not None else "-", help=help_text)


def currency(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"${value:,.0f}"


def number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.0f}"


def load_dimensions(config: SnowflakeConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    facilities = run_query(
        f"""
        SELECT facility_id, facility_name, region, state
        FROM {relation(config, "DIM_FACILITY")}
        ORDER BY facility_name
        """,
        config,
    )
    payers = run_query(
        f"""
        SELECT payer_id, payer_name, payer_category, contract_type
        FROM {relation(config, "DIM_PAYER")}
        ORDER BY payer_name
        """,
        config,
    )
    collectors = run_query(
        f"""
        SELECT collector_id, collector_name, manager_name, region, active_flag
        FROM {relation(config, "DIM_COLLECTOR")}
        ORDER BY collector_name
        """,
        config,
    )
    return facilities, payers, collectors


def in_filter(column: str, values: list[Any]) -> str:
    if not values:
        return "1=1"
    formatted = []
    for value in values:
        if isinstance(value, str):
            formatted.append("'" + value.replace("'", "''") + "'")
        else:
            formatted.append(str(value))
    return f"{column} IN ({', '.join(formatted)})"


def build_filters(facilities: pd.DataFrame, payers: pd.DataFrame) -> tuple[str, list[int], list[int]]:
    with st.sidebar:
        st.header("Filters")
        facility_labels = {
            f"{row.FACILITY_NAME} ({row.REGION})": int(row.FACILITY_ID)
            for row in facilities.itertuples()
        }
        payer_labels = {
            f"{row.PAYER_NAME} ({row.PAYER_CATEGORY})": int(row.PAYER_ID)
            for row in payers.itertuples()
        }

        selected_facilities = st.multiselect(
            "Facilities",
            options=list(facility_labels.keys()),
            placeholder="All facilities",
        )
        selected_payers = st.multiselect(
            "Payers",
            options=list(payer_labels.keys()),
            placeholder="All payers",
        )

        facility_ids = [facility_labels[item] for item in selected_facilities]
        payer_ids = [payer_labels[item] for item in selected_payers]

    where = " AND ".join(
        [
            in_filter("facility_id", facility_ids),
            in_filter("payer_id", payer_ids),
        ]
    )
    return where, facility_ids, payer_ids


def render_ar_overview(config: SnowflakeConfig, where_clause: str) -> None:
    summary = run_query(
        f"""
        SELECT
            COUNT(DISTINCT claim_id) AS claim_count,
            COUNT(DISTINCT account_id) AS account_count,
            SUM(billed_amount) AS billed_amount,
            SUM(balance_amount) AS balance_amount,
            SUM(IFF(is_denied, 1, 0)) AS denied_claims
        FROM {relation(config, "FCT_AR_AGING_SNAPSHOT")}
        WHERE {where_clause}
        """,
        config,
    )
    row = summary.iloc[0]

    cols = st.columns(5)
    with cols[0]:
        metric_card("Claims", number(row.CLAIM_COUNT))
    with cols[1]:
        metric_card("Accounts", number(row.ACCOUNT_COUNT))
    with cols[2]:
        metric_card("Billed", currency(row.BILLED_AMOUNT))
    with cols[3]:
        metric_card("Open Balance", currency(row.BALANCE_AMOUNT))
    with cols[4]:
        metric_card("Denied Claims", number(row.DENIED_CLAIMS))

    status = run_query(
        f"""
        SELECT
            claim_status,
            COUNT(*) AS claims,
            SUM(balance_amount) AS balance_amount
        FROM {relation(config, "FCT_AR_AGING_SNAPSHOT")}
        WHERE {where_clause}
        GROUP BY claim_status
        ORDER BY claims DESC
        """,
        config,
    )

    by_facility = run_query(
        f"""
        SELECT
            d.facility_name,
            d.region,
            COUNT(*) AS claims,
            SUM(f.balance_amount) AS balance_amount
        FROM {relation(config, "FCT_AR_AGING_SNAPSHOT")} f
        LEFT JOIN {relation(config, "DIM_FACILITY")} d USING (facility_id)
        WHERE {where_clause}
        GROUP BY d.facility_name, d.region
        ORDER BY balance_amount DESC
        LIMIT 15
        """,
        config,
    )

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Claim Status")
        chart = (
            alt.Chart(status)
            .mark_bar()
            .encode(
                x=alt.X("CLAIMS:Q", title="Claims"),
                y=alt.Y("CLAIM_STATUS:N", title=None, sort="-x"),
                color=alt.Color("CLAIM_STATUS:N", legend=None),
                tooltip=["CLAIM_STATUS", "CLAIMS", alt.Tooltip("BALANCE_AMOUNT:Q", format="$,.0f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    with right:
        st.subheader("Balance by Facility")
        chart = (
            alt.Chart(by_facility)
            .mark_bar()
            .encode(
                x=alt.X("BALANCE_AMOUNT:Q", title="Open Balance"),
                y=alt.Y("FACILITY_NAME:N", title=None, sort="-x"),
                color=alt.Color("REGION:N", title="Region"),
                tooltip=[
                    "FACILITY_NAME",
                    "REGION",
                    "CLAIMS",
                    alt.Tooltip("BALANCE_AMOUNT:Q", format="$,.0f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)


def render_denials(config: SnowflakeConfig, where_clause: str) -> None:
    denials = run_query(
        f"""
        SELECT
            denial_bucket,
            COUNT(*) AS denial_count,
            SUM(adjustment_amount) AS adjustment_amount
        FROM {relation(config, "FCT_DENIAL_ANALYSIS")}
        WHERE {where_clause}
        GROUP BY denial_bucket
        ORDER BY denial_count DESC
        """,
        config,
    )

    payer_denials = run_query(
        f"""
        SELECT
            p.payer_name,
            p.payer_category,
            COUNT(*) AS denial_count,
            SUM(d.adjustment_amount) AS adjustment_amount
        FROM {relation(config, "FCT_DENIAL_ANALYSIS")} d
        LEFT JOIN {relation(config, "DIM_PAYER")} p USING (payer_id)
        WHERE {where_clause}
        GROUP BY p.payer_name, p.payer_category
        ORDER BY denial_count DESC
        LIMIT 15
        """,
        config,
    )

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Denial Buckets")
        chart = (
            alt.Chart(denials)
            .mark_arc(innerRadius=55)
            .encode(
                theta=alt.Theta("DENIAL_COUNT:Q"),
                color=alt.Color("DENIAL_BUCKET:N", title="Bucket"),
                tooltip=[
                    "DENIAL_BUCKET",
                    "DENIAL_COUNT",
                    alt.Tooltip("ADJUSTMENT_AMOUNT:Q", format="$,.0f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    with right:
        st.subheader("Top Payers by Denials")
        chart = (
            alt.Chart(payer_denials)
            .mark_bar()
            .encode(
                x=alt.X("DENIAL_COUNT:Q", title="Denials"),
                y=alt.Y("PAYER_NAME:N", title=None, sort="-x"),
                color=alt.Color("PAYER_CATEGORY:N", title="Category"),
                tooltip=[
                    "PAYER_NAME",
                    "PAYER_CATEGORY",
                    "DENIAL_COUNT",
                    alt.Tooltip("ADJUSTMENT_AMOUNT:Q", format="$,.0f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)


def render_remits(config: SnowflakeConfig, where_clause: str) -> None:
    remit_daily = run_query(
        f"""
        SELECT
            payment_date,
            SUM(paid_amount) AS paid_amount,
            SUM(adjustment_amount) AS adjustment_amount,
            COUNT(*) AS remit_count
        FROM {relation(config, "FCT_REMIT_ADJUSTMENTS")}
        WHERE {where_clause}
        GROUP BY payment_date
        ORDER BY payment_date
        """,
        config,
    )

    remit_status = run_query(
        f"""
        SELECT
            remit_status,
            COUNT(*) AS remit_count,
            SUM(paid_amount) AS paid_amount
        FROM {relation(config, "FCT_REMIT_ADJUSTMENTS")}
        WHERE {where_clause}
        GROUP BY remit_status
        ORDER BY remit_count DESC
        """,
        config,
    )

    st.subheader("Payments and Adjustments")
    chart = (
        alt.Chart(remit_daily)
        .transform_fold(["PAID_AMOUNT", "ADJUSTMENT_AMOUNT"], as_=["Metric", "Amount"])
        .mark_line(point=True)
        .encode(
            x=alt.X("PAYMENT_DATE:T", title="Payment Date"),
            y=alt.Y("Amount:Q", title="Amount"),
            color=alt.Color("Metric:N", title=None),
            tooltip=[
                alt.Tooltip("PAYMENT_DATE:T", title="Payment Date"),
                "Metric:N",
                alt.Tooltip("Amount:Q", format="$,.0f"),
            ],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(remit_status, use_container_width=True, hide_index=True)


def render_collectors(config: SnowflakeConfig) -> None:
    productivity = run_query(
        f"""
        SELECT
            c.collector_name,
            c.manager_name,
            c.region,
            p.action_date,
            p.action_count,
            p.note_count,
            p.worked_count
        FROM {relation(config, "FCT_COLLECTOR_PRODUCTIVITY_DAILY")} p
        LEFT JOIN {relation(config, "DIM_COLLECTOR")} c USING (collector_id)
        ORDER BY p.action_date, c.collector_name
        """,
        config,
    )

    top_collectors = (
        productivity.groupby(["COLLECTOR_NAME", "MANAGER_NAME", "REGION"], dropna=False)[
            ["ACTION_COUNT", "NOTE_COUNT", "WORKED_COUNT"]
        ]
        .sum()
        .reset_index()
        .sort_values("WORKED_COUNT", ascending=False)
        .head(15)
    )

    st.subheader("Collector Productivity")
    chart = (
        alt.Chart(top_collectors)
        .mark_bar()
        .encode(
            x=alt.X("WORKED_COUNT:Q", title="Worked Accounts"),
            y=alt.Y("COLLECTOR_NAME:N", title=None, sort="-x"),
            color=alt.Color("REGION:N", title="Region"),
            tooltip=["COLLECTOR_NAME", "MANAGER_NAME", "REGION", "ACTION_COUNT", "NOTE_COUNT", "WORKED_COUNT"],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(top_collectors, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(
        page_title="Claims Mart Dashboard",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Claims Mart Dashboard")
    st.caption("Snowflake marts built by dbt through Airflow/Cosmos")

    config = get_config()
    facilities, payers, _ = load_dimensions(config)
    where_clause, _, _ = build_filters(facilities, payers)

    tab_ar, tab_denials, tab_remits, tab_collectors, tab_data = st.tabs(
        ["AR Overview", "Denials", "Remits", "Collectors", "Data"]
    )
    with tab_ar:
        render_ar_overview(config, where_clause)
    with tab_denials:
        render_denials(config, where_clause)
    with tab_remits:
        render_remits(config, where_clause)
    with tab_collectors:
        render_collectors(config)
    with tab_data:
        st.subheader("Mart Samples")
        sample = run_query(
            f"""
            SELECT *
            FROM {relation(config, "FCT_AR_AGING_SNAPSHOT")}
            WHERE {where_clause}
            LIMIT 200
            """,
            config,
        )
        st.dataframe(sample, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
