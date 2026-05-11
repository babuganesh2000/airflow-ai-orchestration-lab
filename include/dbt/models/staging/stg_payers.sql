select
    payer_id::number as payer_id,
    payer_name::varchar as payer_name,
    payer_category::varchar as payer_category,
    contract_type::varchar as contract_type
from {{ source('raw', 'payers_raw') }}
