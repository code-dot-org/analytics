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
        , case 
            when early_access = 1 then 'early access'
            else null
        end                                         as details
        , created_at
    from feedbacks 
)

select * 
from renamed 
