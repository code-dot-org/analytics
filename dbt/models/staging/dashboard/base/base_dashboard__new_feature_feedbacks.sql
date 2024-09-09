with 
source as (
    select * 
    from {{ source('dashboard', 'new_feature_feedbacks') }}
)

select * 
from source