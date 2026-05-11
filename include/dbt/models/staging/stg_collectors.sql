select
    collector_id::number as collector_id,
    collector_name::varchar as collector_name,
    manager_name::varchar as manager_name,
    region::varchar as region,
    active_flag::varchar as active_flag
from {{ source('raw', 'collectors_raw') }}
