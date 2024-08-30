with 

trained_teachers as (
    select distinct
        teacher_id
        , cal_year
        , workshop_organizer
        , school_country
    from {{ ref('dim_global_teacher_roster') }}
),

levels_per_script as (
    select 
        script_id
        , count(distinct level_id)                              as num_levels_in_script 
    from {{ ref('dim_course_structure') }}
    where level_type != 'StandaloneVideo'
    group by 1
),

student_activity as (
    select 
        student_id
        , script_id
        , script_name
        , extract (year from activity_date)                     as cal_year
        , section_teacher_id
        , count(distinct level_id)                              as num_levels_attempted
    from {{ ref('dim_student_script_level_activity') }}
    where level_type != 'StandaloneVideo'
    and section_teacher_id in (select teacher_id from trained_teachers)
    {{ dbt_utils.group_by(5) }}
),

final as (
    select 
        tt.teacher_id
        , sa.cal_year
        , tt.cal_year                                            as cal_year_trained
        , tt.workshop_organizer 
        , tt.school_country
        , sa.script_id
        , sa.script_name
        , ls.num_levels_in_script
        , avg(sa.num_levels_attempted)                          as avg_levels_attempted
        , max(sa.num_levels_attempted)                          as highest_level_attempted
        , count(distinct sa.student_id)                         as num_students_with_activity
    from trained_teachers                                       as tt
    join student_activity                                       as sa
        on tt.teacher_id = sa.section_teacher_id
    left join levels_per_script                                 as ls
        on sa.script_id = ls.script_id
    {{ dbt_utils.group_by(8) }} 
)

select * 
from final