select
    remit_id::number(38,0) as remit_id,
    claim_id::number(38,0) as claim_id,
    payment_date::date as payment_date,
    paid_amount::number(18,2) as paid_amount,
    adjustment_amount::number(18,2) as adjustment_amount,
    nullif(trim(carc_code), '') as carc_code,
    nullif(trim(rarc_code), '') as rarc_code,
    upper(trim(remit_status)) as remit_status,
    load_ts::timestamp_ntz as load_ts,
    batch_id::varchar as batch_id,
    src_file_name::varchar as src_file_name
from {{ source('raw', 'remittances_raw') }}