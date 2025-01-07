with 

amb_section_metrics as (
    select 
        user_id,
        code_studio_name,
        email,
        school_year,
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
            user_id,
            code_studio_name,
            email,
            pivot(response_text for item_name in ('event_date'))
        -- case 
        --     when item_name = 'event_date' then response_text
        --     else null 
        -- end as event_date,
        
        -- case 
        --     when item_name = 'event_type' then response_value
        --     else null 
        -- end as event_type,
        
        -- case 
        --     when item_name = 'pre_enrollment' then response_text
        --     else null 
        -- end as pre_enrollment,
        
        -- case 
        --     when item_name = 'post_enrollment' then response_text
        --     else null 
        -- end as post_enrollment,

        -- case 
        --     when item_name = 'total_participants' then response_text
        --     else null 
        -- end as total_participants
    from {{ ref('dim_foorms') }}
    where form_name = 'surveys/teachers/cs_ambassador_event'
),

event_impact_coalesce as (
    select 
        user_id,
        code_studio_name,
        email,
    from event_impact_survey

)

select 
    asm.user_id,
    asm.code_studio_name,
    asm.email,
    asm.school_year,
    case when eis.event_type
    asm.num_sections,
    asm.num_students,
    scl.courses_touched,


from amb_section_metrics                    as asm 

left join section_course_list               as scl 
    on asm.user_id = scl.user_id 
    and asm.school_year = scl.school_year

left join event_impact_survey               as eis
    on asm.user_id = eis.user_id
    and asm.school_year = eis.school_year


