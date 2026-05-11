select
    facility_id::number as facility_id,
    facility_name::varchar as facility_name,
    state::varchar as state,
    region::varchar as region
from {{ source('raw', 'facilities_raw') }}
