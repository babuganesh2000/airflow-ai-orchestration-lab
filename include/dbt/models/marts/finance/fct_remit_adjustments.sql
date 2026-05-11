select
    remit_id,
    claim_id,
    account_id,
    facility_id,
    payer_id,
    payment_date,
    paid_amount,
    adjustment_amount,
    carc_code,
    rarc_code,
    remit_status
from {{ ref('int_remit_claim_alignment') }}
