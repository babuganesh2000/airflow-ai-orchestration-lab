select
    collector_id,
    collector_name,
    manager_name,
    region,
    active_flag
from {{ ref('stg_collectors') }}
