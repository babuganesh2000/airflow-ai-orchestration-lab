select
    account_id,
    collector_id,
    assignment_status,
    queue_name,
    updated_at
from {{ ref('stg_accounts') }}
