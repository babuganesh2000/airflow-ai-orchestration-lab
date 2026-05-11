select
    action_id::number(38,0) as action_id,
    account_id::number(38,0) as account_id,
    collector_id::number(38,0) as collector_id,
    action_date::date as action_date,
    action_type::varchar as action_type,
    note_count::number(38,0) as note_count,
    worked_flag::varchar as worked_flag,
    load_ts::timestamp_ntz as load_ts,
    batch_id::varchar as batch_id,
    src_file_name::varchar as src_file_name
from {{ source('raw', 'workqueue_actions_raw') }}