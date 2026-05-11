{% snapshot collector_assignment_snapshot %}

{{
    config(
        target_schema='snapshot',
        unique_key='account_id',
        strategy='check',
        check_cols=['collector_id', 'assignment_status', 'queue_name'],
        invalidate_hard_deletes=true
    )
}}

with latest_account_state as (

    select
        account_id,
        collector_id,
        assignment_status,
        queue_name,
        updated_at,
        load_ts,
        row_number() over (
            partition by account_id
            order by updated_at desc, load_ts desc
        ) as rn
    from {{ ref('stg_accounts') }}

)

select
    account_id,
    collector_id,
    assignment_status,
    queue_name,
    updated_at
from latest_account_state
where rn = 1

{% endsnapshot %}