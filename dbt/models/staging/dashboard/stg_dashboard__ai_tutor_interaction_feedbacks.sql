with 

base as ( 
    select * 
    from {{ ref('base_dashboard__ai_tutor_interaction_feedbacks') }}
)

select 
    id                                          as feedback_id
    , 'ai_tutor'                                as feature_name
    , user_id 
    , case 
        when thumbs_up = 1 then 1 
        when thumbs_down = 1 then 0 
        else null 
    end                                         as thumbs_up
    , details 
    , created_at
from base 