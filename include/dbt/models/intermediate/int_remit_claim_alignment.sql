with remits as (
    select * from {{ ref('stg_remittances') }}
),
claims as (
    select * from {{ ref('stg_claims') }}
)

select
    r.remit_id,
    r.claim_id,
    c.account_id,
    c.facility_id,
    c.payer_id,
    r.payment_date,
    r.paid_amount,
    r.adjustment_amount,
    r.carc_code,
    r.rarc_code,
    r.remit_status
from remits r
left join claims c
    on r.claim_id = c.claim_id
