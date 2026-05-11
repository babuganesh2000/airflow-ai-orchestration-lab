select
    facility_id,
    facility_name,
    state,
    region
from {{ ref('stg_facilities') }}
