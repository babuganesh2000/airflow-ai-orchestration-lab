with claims as (
    select * from {{ ref('stg_claims') }}
),
accounts as (
    select * from {{ ref('stg_accounts') }}
)

select
    c.claim_id,
    c.account_id,
    c.facility_id,
    c.payer_id,
    c.service_date,
    c.billed_amount,
    c.claim_status,
    a.current_balance,
    a.collector_id,
    a.assignment_status,
    a.queue_name,
    case
        when c.claim_status = 'DENIED' then true
        else false
    end as is_denied
from claims c
left join accounts a
    on c.account_id = a.account_id
