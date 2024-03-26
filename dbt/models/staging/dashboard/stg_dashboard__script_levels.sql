with 
script_levels as (
    select * 
    from {{ ref('base_dashboard__script_levels')}}
),

final as (
    select 
        script_level_id,
        script_id,
        chapter,
        stage_id,
        position,
        is_assessment,
        is_named_level,
        is_bonus,
        
        case when json_extract_path_text(
            properties,
            'challenge') 
            = 'true'
            then 1 else 0 end             as is_challenge,

        activity_section_id,
        activity_section_position,
        seed_key,
        -- properties,
        created_at,
        updated_at
    from script_levels)

select * 
from final 