{# Notes:
Design: 1 row per teacher, school_year, churn_status
Logic: we can determine status based on three properties we can compute for every user|school_year as a binary:
    - 0/1 they are active this school_year - (A)ctive
    - 0/1 they were active in the previous school_year - (P)rev year
    - 0/1 they have ever been active in ANY school_year prior, incl. prev year - (E)ver before

    These 3 values can be combined into an ordered 3-char string representing the concatenated true/false combinations 
    for Active|Prev|Ever e.g. "101" means: ( Active = true AND Prev year = false AND Ever before = true )

    - '000' (0) = 'market'              -- Not active now + never been active
    - '001' (1) = 'inactive churn'      -- NOT active + NOT active prev year + active ever before
    - '010' (2) = '<impossible status>' -- should not be possible, active in the prev year should imply active ever before
    - '011' (3) = 'inactive this year'  -- NOT active + active prev year + (active ever before implied)
    - '100' (4) = 'active new'          -- active this year + NOT active last year + NOT active ever before
    - '101' (5) = 'active reacquired'   -- Active this year + NOT active last year + active in the past
    - '110' (6) = '<impossible status>' -- impossible for same reason as status (2)
    - '111' (7) = 'active retained'     -- active this year + active last year + (active ever before implied) 
#}

with 
teacher_section_started as (
    select teacher_id,
        school_year,
        min(section_started_at) as started_teaching_at,
        listagg(distinct course_name, ', ') within group (order by course_name ASC) section_courses_started
    from {{ ref('int_active_sections') }}
    group by 1, 2
),

all_teacher_users as (
    select
        teacher_id,
        created_at
    from {{ref('dim_teachers')}}
), 

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

all_teachers_school_years as (
    select
        u.teacher_id,
        sy.school_year
    from all_teacher_users u
    join school_years sy on u.created_at <= sy.ended_at
    where sy.started_at < current_timestamp
), 

active_status_simple as (
    select
        all_sy.teacher_id,
        all_sy.school_year,
        case when t.teacher_id is null then 0 else 1 end as is_active,
        t.section_courses_started,
        t.started_teaching_at
    from all_teachers_school_years all_sy
    left join teacher_section_started t 
        on t.teacher_id = all_sy.teacher_id 
        and t.school_year = all_sy.school_year
), 

full_status as (
    -- Determine the active status for each teacher in each year
    select
        teacher_id,
        school_year,
        is_active,
        section_courses_started,
        started_teaching_at,
        coalesce(
            lag(is_active, 1) 
                over (partition by teacher_id order by school_year) 
            , 0
        ) as prev_year_active,
        coalesce( --force any NULL to be 0 for this function
            max(is_active) 
                over (partition by teacher_id order by school_year rows between unbounded preceding and 1 preceding)
            , 0
        ) as ever_active_before,
        (is_active || prev_year_active || ever_active_before) status_code
    from
        active_status_simple
), 

final as (
    select
        teacher_id,
        school_year,
        case 
            when status_code = '000' then 'market'
            when status_code = '001' then 'inactive churn'
            when status_code = '010' then '<impossible status>'
            when status_code = '011' then 'inactive this year'
            when status_code = '100' then 'active new'
            when status_code = '101' then 'active reacquired'
            when status_code = '110' then '<impossible status>'
            when status_code = '111' then 'active retained'
        end as status,
        section_courses_started,
        started_teaching_at
    from
        full_status
    order by
        teacher_id, school_year
)

select * 
from final