with 

pd_intl_opt_ins as (
    select 
        * 
        , row_number() over (
            partition by teacher_id 
            order by form_submitted_at desc 
        )                                                               as app_num
    from {{ ref('stg_dashboard_pii__pd_international_opt_ins') }}
), 

teachers as (
    select * 
    from {{ ref('dim_teachers') }}
    where teacher_id in (select distinct teacher_id from pd_intl_opt_ins)
),

student_activity as (
    select * 
    from {{ ref('dim_student_script_level_activity') }}
    where section_teacher_id in (select distinct teacher_id from pd_intl_opt_ins)
),

teachers_started as (
    select 
        teacher_id 
    from {{ ref('int_active_sections') }}
    where teacher_id in (select distinct teacher_id from pd_intl_opt_ins)
),

--flag student activity as taking place before or after the teacher was trained
training_impact_flags as (
    select 
        section_teacher_id
        , sa.student_id
        , sa.activity_date
        , case
            when sa.activity_date < oi.form_submitted_at then student_id 
            else null                  
        end                                                         as pre_training_student
        , case 
            when sa.activity_date >= oi.form_submitted_at then student_id 
            else null                  
        end                                                         as post_training_student
    from pd_intl_opt_ins                                            as oi
    left join student_activity                                      as sa 
        on sa.section_teacher_id = oi.teacher_id
)

select 
    oi.application_id
    , oi.teacher_id 
    , case 
        when oi.app_num = 1 then 1 
        else 0                                                  
    end                                                         as most_recent_app
    , oi.form_submitted_at
    , oi.cal_year
    , oi.first_name
    , oi.last_name
    , oi.pref_name 
    , oi.email 
    , oi.email_alt
    , oi.school_department
    , oi.school_municipality
    , oi.school_name
    , oi.school_city 
    , oi.school_country 
    , oi.workshop_date 
    , oi.workshop_organizer 
    , oi.workshop_course_name 
    , oi.email_opt_in
    , t.created_at                                              as account_created_at   
    , t.current_sign_in_at 
    , t.sign_in_count
    , case 
        when ts.teacher_id is not null then 1 
        else 0
    end                                                         as implemented
    , count(distinct tif.pre_training_student)                  as pre_training_num_students
    , count(distinct tif.post_training_student)                 as post_training_num_students
    , count(distinct tif.student_id)                            as total_students                                        
from pd_intl_opt_ins                                            as oi 
left join teachers                                              as t 
    on oi.teacher_id = t.teacher_id
left join training_impact_flags                                 as tif 
    on tif.section_teacher_id = oi.teacher_id
left join teachers_started                                      as ts 
    on ts.teacher_id = oi.teacher_id 
{{ dbt_utils.group_by(23) }}