with
scripts as (
    select
        script_id,
        script_name,
        created_at,
        updated_at,
        wrapup_video_id,
        user_id,
        login_required,
        new_name,
        family_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience,
        case
            when lower(script_name) like 'devices-20__'                         then 'csd'
            when lower(script_name) like 'microbit%'                            then 'csd'
            when lower(script_name) like '%hello%'                              then 'hoc'
            when lower(script_name) like 'csd-post-survey-20__'                 then 'csd'
            when lower(script_name) like 'csp-post-survey-20__'                 then 'csp'
            when
                json_extract_path_text(properties, 'curriculum_umbrella') = ''  then 'other'
            else
                lower(json_extract_path_text(properties, 'curriculum_umbrella'))
        end as course_name_true,
        json_extract_path_text(properties, 'supported_locales') as supported_locales
    from {{ ref('base_dashboard__scripts') }}
)
select * from scripts
