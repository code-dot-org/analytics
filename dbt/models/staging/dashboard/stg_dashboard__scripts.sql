with
scripts as (
    select 
        *,
        {{ clean_json_array('topic_tags') }} as topic_tags_cleaned
    from {{ ref('base_dashboard__scripts') }}
),

renamed as (
    select *,
        listagg(distinct topic_tags_cleaned) within group(order by topic_tags_cleaned asc) as topic_tags_list
    from scripts 
    {{ dbt_utils.group_by(21)}}
),

final as (
    select 
        script_id, 
        script_name,
        wrapup_video_id,
        user_id,
        login_required,
        new_name,
        family_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience,
        course_name,
        supported_locales,
        version_year,
        is_standalone,
        unit,
        nullif(content_area,'')     as content_area,
        nullif(topic_tags_list,'')  as topic_tags,
        created_at,
        updated_at
    from renamed )

select * 
from final 
