with 

pl_activity as ( 
    select *
    from {{ ref('dim_pl_activity') }}
),

school_years as ( 
    select * 
    from {{ ref('int_school_years') }}
),

active_teachers as ( 
    select * 
    from {{ ref('dim_teacher_status') }}
    where status like 'active%'
),

active_teachers_sy_int as (
    select 
        active_teachers.*,
        school_years.school_year_int 
    from active_teachers 
    join school_years 
        on active_teachers.school_year = school_years.school_year 
),
    
-- active_students_by_teacher as ( 
--     select 
--         active_students.teacher_id,
--         active_students.school_year,
--         school_years.school_year_int,
--         count(distinct active_students.student_id):: float as num_active_students
--     from {{ ref('dim_active_students') }} active_students
--     join school_years 
--         on active_students.school_year = school_years.school_year
--     group by 1,2,3
-- ),

pl_with_engagement as (
    select 
        pl_activity.teacher_id,
        pl_activity.us_intl,
        pl_activity.school_year,
        school_years.school_year_int,
        pl_activity.grade_band,
        pl_activity.school_id,
        pl_activity.school_district_id,
        sum(coalesce(pl_activity.num_hours,0)) as total_hours,
        sum(coalesce(pl_activity.num_levels,0)) as total_levels,

        case 
            when coalesce(sum(pl_activity.num_hours), 0) = 0 and coalesce(sum(pl_activity.num_levels), 0) < 37 then 'low'
            when (sum(pl_activity.num_hours) between 1 and 8) or (sum(pl_activity.num_levels) between 37 and 65) then 'medium'
            when (sum(pl_activity.num_hours) > 8) or (sum(pl_activity.num_levels) > 65) then 'high'
            else null 
        end as pl_engagement_level,

        case 
            when sum(pl_activity.num_hours) > 0 then true 
            else false
        end as includes_facilitated,

        listagg(distinct pl_activity.topic, ', ') within group (order by pl_activity.teacher_id, pl_activity.school_year, pl_activity.grade_band) as topics_touched
    from pl_activity 
    join school_years 
        on pl_activity.school_year = school_years.school_year
    group by 1,2,3,4,5,6,7
)

select 
    pl.teacher_id,
    pl.us_intl,
    pl.school_year,
    pl.grade_band,
    pl.school_id,
    pl.school_district_id,
    pl.total_hours,
    pl.total_levels,
    pl.pl_engagement_level,
    pl.includes_facilitated,
    pl.topics_touched,
    case 
        when act_1.teacher_id is not null or act_2.teacher_id is not null then 1.00 
        else 0.00  
    end as implemented,
    case 
        when act_1.teacher_id is not null and act_2.teacher_id is not null then 1.00 
        when act_2.teacher_id is not null and act_3.teacher_id is not null then 1.00
        else 0.00
    end as sustained,
    case 
        when act_1.teacher_id is not null then act_1.school_year
        when act_2.teacher_id is not null then act_2.school_year 
        else null
    end as first_year_implemented
    -- case 
    --     when act_1.teacher_id is not null then act_stu_1.num_active_students 
    --     when act_2.teacher_id is not null then act_stu_2.num_active_students 
    --     else 0.00
    -- end as num_active_students
from pl_with_engagement pl
left join active_teachers_sy_int act_1
    on pl.teacher_id = act_1.teacher_id 
    and pl.school_year_int = act_1.school_year_int
left join active_teachers_sy_int act_2
    on pl.teacher_id = act_2.teacher_id 
    and pl.school_year_int + 1 = act_2.school_year_int
left join active_teachers_sy_int act_3
    on pl.teacher_id = act_3.teacher_id 
    and pl.school_year_int + 2 = act_3.school_year_int
-- left join active_students_by_teacher act_stu_1
--     on pl.teacher_id = act_stu_1.teacher_id
--     and pl.school_year_int = act_stu_1.school_year_int
-- left join active_students_by_teacher act_stu_2
--     on pl.teacher_id = act_stu_2.teacher_id 
--     and pl.school_year_int + 1 = act_stu_2.school_year_int




