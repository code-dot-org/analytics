with projects as (
    select *
    from {{ ref('base_dashboard_pii__projects') }}
),

final as (
    select 
        project_id,
        storage_id,
        project_type,

        value,
        state,
        remix_parent_id,
        abuse_score,
        
        -- flags
        is_standalone,
        is_skip_content_moderation,
        
        -- dates
        published_at,
        created_at,
        updated_at,
        case
            when json_extract_path_text(value, 'id', true) <> '' 
            then 1 
            else 0 
        end                                                                     as is_valid

    from projects )

select *
from final
where is_valid = 1