-- Model: dim_school_course_status
-- Scope: course status data collected for active courses only collected by school
-- Author: js

with 
teachers as (
    select school_id,
        school_year,
        course_name,
        null as num_students_started,
        count(distinct case when started_at is not null then teacher_user_id end) as num_teachers_started,
        min(started_at) as first_started_at,
        dense_rank() over(partition by school_id, course_name order by school_year asc) as sequence_num,
        dense_rank() over(partition by school_id, course_name order by school_year desc) as sequence_num_inv
    from {{ ref('dim_teachers') }}
    where started_at is not null 
    {{ dbt_utils.dbt_utils.group_by('4')}}
),

students as (
    select school_id,
        school_year,
        course_name,
        count(distinct case when started_at is not null then teacher_user_id end) as num_users_started,
    from {{ ref('dim_students') }}
    where started_at is not null 
    {{ dbt_utils.dbt_utils.group_by('3')}}
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

combined as (
    select 
        teachers.school_id,
        teachers.school_year,
        teachers.course_name, -- really need to update this to course_id for FK joins
        students.num_students_started,
        teachers.num_teachers_started,
        teachers.first_started_at,
        teachers.seq
    from teachers 
    left join students 
        on teachers.school_id = students.school_id 
        and teachers.school_year = students.school_year 
        and teachers.course_name = students.course_name
),

final as (
    select school_id,
        school_year,
        course_name,

        -- course_status: this logic needs heavy review @bakerfranke @allison-code-dot-org 
        case when seq = 1 then 'active new' 
             when seq > 1 then 'active retained'
             when seq is null 
              and lag(seq,1) over(partition by school_id, course_name order by school_year) is not null 
                then 'inactive this year'
            when seq is null
             and sum(seq) over (partition by school_id, course_name order by school_year rows unbounded preceding) is not null 
             and lag(seq) over (partition by school_id, course_name order by school_year) is not null 
                then 'inactive churn'
            when seq > 1 
             and lag(seq,1) over (partition by school_id, course_name order by school_year) is null 
                then 'active reacquired'
            else 'market'
        end as course_status, 
        
        -- (js) flag based on presence of course activity
        case when seq is not null then 1 else 0 end as is_active,

        -- (js) aggregating here to ensure our grain
        sum(num_students_started) as total_students_started,
        sum(num_teachers_started) as total_teachers_started,
        {# total_teachers_trained,
        total_teachers_trained_this_year, #}

        max(seq) as total_years_active
    from combined
    {{ dbt_utils.group_by('5') }}
)

select * 
from final 