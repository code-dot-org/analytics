with foorm_simple_survey_forms as (
    select * 
    from {{ ref ('base_dashboard_pii__foorm_simple_survey_forms') }}
)

select * 
from foorm_simple_survey_forms