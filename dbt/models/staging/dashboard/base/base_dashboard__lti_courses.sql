with
source as (
    select * 
    from {{ source('dashboard', 'lti_courses') }}
),

renamed as (
    select 
        id as lti_course_id,
        lti_integration_id,
        lti_deployment_id,
        context_id,
        course_id,
        {# nrps_url, #}
        {# resource_link_id, #}
        created_at,
        updated_at,
        deleted_at
    from source)

select * 
from renamed