with 
scripts as (
    select *
    from {{ ref('base_dashboard__scripts')}}
),

script_names as (
    select * 
    from {{ ref('seed_script_names') }}
),

combined as (
    select 
        script_names.versioned_script_id                                as script_id,
        coalesce(sn.script_name_short,sc.script_name)                   as script_name,
        case 
            when lower(script_name) like 'csa%'then 'csa'
            when lower(script_name) like 'csd%' then 'csd'
            when lower(script_name) like 'csf%' then 'csf'
            when lower(script_name) like 'csp%' then 'csp'
            when lower(script_name) like 'hoc%' then 'hoc'
            when lower(script_name) like 'devices-20__' then 'csd'
            when lower(script_name) like 'microbit' then 'csd'
            else lower(json_extract_path_text(properties,'curriculum_umbrella'))
        end                                                             as course_name_true,
        created_at,
        updated_at,
        wrapup_video_id,
        user_id,
        login_required,
        --properties, --will parse out only what we need (js)
        json_extract_path_text(properties, 'supported_locales')         as supported_locales,
        new_name,
        family_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience
    from scripts as sc 
    left join script_names as sn 
        on sc.script_id = sn.versioned_script_id
)

select * 
from combined