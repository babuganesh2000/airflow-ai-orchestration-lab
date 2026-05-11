select
    collector_id,
    action_date,
    count(*) as action_count,
    sum(note_count) as note_count,
    sum(case when worked_flag = 'Y' then 1 else 0 end) as worked_count
from {{ ref('stg_workqueue_actions') }}
group by 1,2
