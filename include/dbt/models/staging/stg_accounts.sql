select
    account_id::number(38,0) as account_id,
    patient_id::number(38,0) as patient_id,
    facility_id::number(38,0) as facility_id,
    current_balance::number(18,2) as current_balance,
    assignment_status::varchar as assignment_status,
    collector_id::number(38,0) as collector_id,
    queue_name::varchar as queue_name,
    updated_at::timestamp_ntz as updated_at,
    load_ts::timestamp_ntz as load_ts,
    batch_id::varchar as batch_id,
    src_file_name::varchar as src_file_name
from {{ source('raw', 'accounts_raw') }}