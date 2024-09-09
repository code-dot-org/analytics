with 

feedbacks as ( 
    select * 
    from {{ ref('base_dashboard__new_feature_feedbacks') }}
),

renamed as (
    select 
        id                                          as feedback_id
        , 'new feature'                             as feature_name
        , user_id 
        , satisfied
        , null                                      as details 
        , created_at
    from feedbacks 
)

select * 
from renamed 
