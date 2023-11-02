with 
scripts as (
    select 
        script_id,
        script_name,
        case when lower(script_name) like 'csa%'           then 'csa'
             when lower(script_name) like 'csd%'           then 'csd'
             when lower(script_name) like 'csf%'           then 'csf'
             when lower(script_name) like 'csp%'           then 'csp'
             when lower(script_name) like 'hoc%'           then 'hoc'
             when lower(script_name) like 'devices-20__'   then 'csd'
             when lower(script_name) like 'microbit'       then 'csd'
             else lower(json_extract_path_text(properties,'curriculum_umbrella'))
        end as course_name_true,
        created_at,
        updated_at,
        wrapup_video_id,
        user_id,
        login_required,
        json_extract_path_text(properties, 'supported_locales') as supported_locales,
        new_name,
        family_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience
    from {{ ref('base_dashboard__scripts')}}
)

select * from scripts