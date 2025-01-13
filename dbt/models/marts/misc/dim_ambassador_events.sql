with 

amb_section_metrics as (
    select 
        user_id,
        code_studio_name,
        email,
        school_year,
        min(section_created_dt) as section_created_dt,
        count(distinct section_id) as num_sections,
        count(distinct student_id) as num_students
    from {{ ref('dim_ambassador_activity') }}
    group by 1,2,3,4
    having num_sections > 0 
),

section_course_list as (
    select 
        user_id,
        school_year,
        listagg(distinct course_name, ', ') within group (order by user_id, school_year) as courses_touched
    from {{ ref('dim_ambassador_activity') }}
    group by 1,2
),

event_impact_survey as ( 
    select *
    from (select 
            foorm_submission_id, user_id, school_year, item_name, response_text
            from {{ ref('dim_foorms') }}
            where form_name = 'surveys/teachers/cs_ambassador_event')
    pivot(max(response_text) for item_name in (
        'event_date', 
        'event_type',
        'total_participants', 
        'num_pre_enrollment',
        'num_post_enrollment'
        )
    )
)

select 
    asm.user_id,
    asm.code_studio_name,
    asm.email,
    asm.school_year,
    case 
        when eis.event_type = 'experience cs (in code studio)' then 'experience_cs'
        when eis.event_type = 'connect with cs (not in code studio)' then 'connect with cs'
        else 
            case 
                when asm.num_sections > 0 then 'experience_cs'
                else eis.event_type
            end
    end as event_type,
    coalesce(to_timestamp(eis.event_date, 'YYYY-MM-DD HH24:MI:SS'), date_trunc('day', asm.section_created_dt)) as event_date,
    cast(eis.total_participants as int) as survey_total_participants,
    cast(eis.num_pre_enrollment as int) as survey_num_pre_enrollment,
    cast(eis.num_post_enrollment as int) as survey_num_post_enrollment,
    case 
        when eis.num_pre_enrollment is not null and eis.num_post_enrollment is not null
        then 1 
        else 0 
    end as impact_eval_flag,
    asm.num_sections,
    asm.num_students as num_students_in_section,
    scl.courses_touched as section_courses_touched

from amb_section_metrics                    as asm 

left join section_course_list               as scl 
    on asm.user_id = scl.user_id 
    and asm.school_year = scl.school_year

left join event_impact_survey               as eis
    on asm.user_id = eis.user_id
    and asm.school_year = eis.school_year


