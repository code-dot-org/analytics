with 
unit_groups as (
    select * 
    from {{ ref('base_dashboard__unit_groups')}}
)

select 
        unit_group_id,
        unit_group_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience,

        json_extract_path_text(
            properties,
            'family_name',
            true) as family_name,
        
        json_extract_path_text(
            properties,
            'version_year',
            true) as version_year,
        
        properties,
        created_at,
        updated_at
from unit_groups