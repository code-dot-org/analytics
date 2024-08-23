with foorm_simple_survey_submissions as (
    select * 
    from {{ ref ('base_dashboard_pii__foorm_simple_survey_submissions') }}
)

select * 
from foorm_simple_survey_submissions