{{ config(
    materialized='incremental',
    unique_key='claim_snapshot_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with base as (
    select
        claim_id,
        account_id,
        facility_id,
        payer_id,
        current_date as snapshot_date,
        billed_amount,
        current_balance as balance_amount,
        claim_status,
        is_denied,
        md5(
            coalesce(claim_id::varchar, '') || '|' ||
            current_date::varchar
        ) as claim_snapshot_key,
        service_date
    from {{ ref('int_claim_grain') }}

    {% if is_incremental() %}
      where service_date >= dateadd(day, -{{ var('late_arrival_lookback_days', 7) }}, current_date)
    {% endif %}
)

select
    claim_snapshot_key,
    snapshot_date,
    claim_id,
    account_id,
    facility_id,
    payer_id,
    billed_amount,
    balance_amount,
    claim_status,
    is_denied
from base
