with 
unit_groups as (
    select * 
    from {{ ref('base_dashboard__unit_groups')}}
),

renamed as (
    select 
        unit_group_id,
        lower(unit_group_name)              as unit_group_name,
        lower(published_state)              as published_state,
        lower(instruction_type)             as instruction_type,
        lower(instructor_audience)          as instructor_audience,
        lower(participant_audience)         as participant_audience,

        json_extract_path_text(
            properties,
            'family_name',
            true) as family_name,
        
        json_extract_path_text(
            properties,
            'version_year',
            true) as version_year,
        
        -- properties,
        created_at,
        updated_at
    from unit_groups ) 

select * 
from renamed