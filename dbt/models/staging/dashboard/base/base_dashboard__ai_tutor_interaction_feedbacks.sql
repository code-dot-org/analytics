with 
source as (
    select * 
    from {{ source('dashboard', 'ai_tutor_interaction_feedbacks') }}
)

select * 
from source