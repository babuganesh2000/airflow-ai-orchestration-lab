select
    claim_id,
    account_id,
    facility_id,
    payer_id,
    payment_date,
    adjustment_amount,
    carc_code,
    rarc_code,
    case
        when carc_code = '45' then 'Contractual'
        when carc_code = '96' then 'Non-covered'
        when carc_code = '197' then 'Authorization'
        when carc_code = '29' then 'Timely Filing'
        when carc_code = 'B7' then 'Provider Issue'
        else 'Unknown'
    end as denial_bucket
from {{ ref('int_remit_claim_alignment') }}
where carc_code is not null
