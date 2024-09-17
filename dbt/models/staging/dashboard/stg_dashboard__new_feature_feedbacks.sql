with 

feedbacks as ( 
    select * 
    from {{ ref('base_dashboard__new_feature_feedbacks') }}
),

renamed as (
    select 
        id                                          as feedback_id
        , 'progress_view'                           as feature_name
        , user_id 
        , satisfied
        , 0                                         as early_access
        , null                                      as message 
        , created_at
    from feedbacks 
)

select * 
from renamed 
