select
    claim_id::number(38,0) as claim_id,
    account_id::number(38,0) as account_id,
    facility_id::number(38,0) as facility_id,
    payer_id::number(38,0) as payer_id,
    service_date::date as service_date,
    billed_amount::number(18,2) as billed_amount,
    upper(trim(claim_status)) as claim_status,
    load_ts::timestamp_ntz as load_ts,
    batch_id::varchar as batch_id,
    src_file_name::varchar as src_file_name
from {{ source('raw', 'claims_raw') }}