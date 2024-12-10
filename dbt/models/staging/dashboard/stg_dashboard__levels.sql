with 
levels as (
    select * 
    from {{ ref('base_dashboard__levels')}}
),

final as (
    select 
        level_id,
        lower(name)    as level_name,
        lower(type)    as level_type,
        level_num,
        ideal_level_source_id,
        user_id,
        game_id,
        md5,
        is_published,
        
        case when json_extract_path_text(
            properties, 
            'mini_rubric', 
            true) = 'true' 
        then 1 else 0 end       as mini_rubric,

        case when json_extract_path_text(
            properties, 
            'free_play', 
            true) = 'true' 
        then 1 else 0 end       as is_free_play,

        json_extract_path_text(
            properties, 
            'project_template_level_name', 
            true)               as project_template_level_name,

        json_extract_path_text(
            properties, 
            'submittable', 
            true)               as is_submittable,
        
        lower(audit_log)        as audit_log,
        lower(notes)            as notes,
        lower(properties)       as properties,
        created_at,
        updated_at
    from levels )

select * 
from final 
