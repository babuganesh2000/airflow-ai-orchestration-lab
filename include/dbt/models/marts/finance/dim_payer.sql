select
    payer_id,
    payer_name,
    payer_category,
    contract_type
from {{ ref('stg_payers') }}
