with 

feedbacks as ( 
    select * 
    from {{ ref('base_dashboard__lti_feedbacks') }}
),

renamed as (
    select 
        id                                          as feedback_id
        , 'lti'                                     as feature_name
        , user_id 
        , satisfied
        , early_access                                        
        , null                                      as message
        , created_at
    from feedbacks 
)

select * 
from renamed 
