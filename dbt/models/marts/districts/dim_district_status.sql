{# Notes:
Design: 1 row per school district, school_year, churn_status
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

Edit log
--Cory, May 2025
1) Added district_started_at field as the date the first school in the district became active


#}

with 

dim_schools as (
    select * 
    from {{ref('dim_schools')}}
),

all_districts as (
    select school_district_id
    from {{ ref('dim_districts') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

districts_enrolled as (
    select * 
    from {{ ref('stg_external_datasets__districts_enrolled') }}
),

districts_target as (
    select * 
    from {{ ref('stg_external_datasets__districts_target') }}
),

all_districts_sy as (
    select 
        all_districts.school_district_id,
        school_years.school_year
    from all_districts 
    cross join school_years
),

teacher_school_changes as (
    select *
    from {{ ref('int_teacher_schools_historical') }}
),

active_schools as (
    select * 
    from {{ref('dim_school_status')}}
    where status in ('active new', 'active retained', 'active reacquired')
),

teacher_active_courses as (
    select 
        distinct teacher_id,
        school_year,
        course_name,
        section_started_at,
        section_active_at
    from {{ref('int_active_sections')}}
),

teacher_active_courses_with_sy as (
    select
        tac.teacher_id,
        tac.school_year,
        tac.course_name,
        tac.section_started_at,
        tac.section_active_at,
        tsc.school_id,
        dim_schools.school_district_id
    from teacher_active_courses tac 
    join school_years sy
        on tac.school_year = sy.school_year
    join teacher_school_changes tsc 
        on tac.teacher_id = tsc.teacher_id 
        and sy.ended_at between tsc.started_at and tsc.ended_at 
    join dim_schools 
        on tsc.school_id = dim_schools.school_id
),

--based on school_started_at not section_started_at to account for teacher/school mappings
--teachers can only be mapped to one school at a time, so safe to sum 
active_district_stats as (
    select 
        school_district_id
        , school_year
        , min(school_started_at)                                                       as district_started_at
        , min(school_active_at)                                                        as district_active_at
        , sum(num_active_teachers)                                                   as num_active_teachers
        , count(distinct active_schools.school_id)                                         as num_active_schools
    from active_schools
    left join dim_schools  
        on dim_schools.school_id = active_schools.school_id
    where school_district_id is not null
    group by 1, 2
),

--listagg cannot be combined with count distinct; active_courses comes from sections not schools to avoid splitting + recombining 
active_district_courses as (
    select 
        school_district_id
        , school_year
        , listagg( distinct course_name, ', ') within group (order by course_name)      as active_courses
    from teacher_active_courses_with_sy
    group by 1, 2
),

active_status_simple as (
    select 
        all_districts_sy.school_district_id,
        all_districts_sy.school_year,
        case 
            when active_district_stats.school_district_id is not null 
            then 1 
            else 0 
        end                                                                 as is_active,
        active_district_stats.district_started_at,
        active_district_stats.district_active_at,
        active_district_courses.active_courses,
        coalesce(active_district_stats.num_active_teachers, 0)              as num_active_teachers,
        coalesce(active_district_stats.num_active_schools, 0)               as num_active_schools
    from all_districts_sy 
    left join active_district_stats
        on all_districts_sy.school_district_id = active_district_stats.school_district_id 
        and all_districts_sy.school_year = active_district_stats.school_year
    left join active_district_courses
        on all_districts_sy.school_district_id = active_district_courses.school_district_id
        and all_districts_sy.school_year = active_district_courses.school_year
)

, full_status as (
    -- Determine the active status for each school district in each school year
    select
        school_district_id
        , school_year
        , is_active
        , coalesce(
            lag(is_active, 1) 
                over (partition by school_district_id order by school_year) 
            , 0
        )                                                                               as prev_year_active
        , coalesce( --force any NULL to be 0 for this function
            max(is_active) 
                over (
                    partition by 
                        school_district_id 
                    order by 
                        school_year 
                    rows between unbounded preceding and 1 preceding)           
            , 0
        )                                                                               as ever_active_before
        , (is_active || prev_year_active || ever_active_before) status_code
        , district_started_at
        , district_active_at
        , active_courses
        , num_active_teachers
        , num_active_schools
    from
        active_status_simple
), 

final as (
    select distinct
        fs.school_district_id
        , fs.school_year
        , case 
            when fs.status_code = '000' then 'market'
            when fs.status_code = '001' then 'inactive churn'
            when fs.status_code = '010' then '<impossible status>'
            when fs.status_code = '011' then 'inactive this year'
            when fs.status_code = '100' then 'active new'
            when fs.status_code = '101' then 'active reacquired'
            when fs.status_code = '110' then '<impossible status>'
            when fs.status_code = '111' then 'active retained'
        end                                                             as status
        , fs.district_started_at
        , fs.district_active_at
        , fs.active_courses
        , fs.num_active_teachers
        , fs.num_active_schools
        , case 
            when de_1.school_year_enrolled is not null then 1 
            else 0 
        end                                                             as is_enrolled
        , case
            when de_2.school_year_enrolled is not null then 1
            else 0 
        end                                                             as is_enrolled_this_year
        , case
            when dt.school_year is not null then 1
            else 0
        end                                                             as is_target_this_year
        , 
        de_1.tier                                                            as current_tier 
        , case
            when de_1.school_year_enrolled is not null then de_1.month_closed
            else null 
        end                                                             as enrolled_at
    from full_status                                                    as fs
    left join districts_enrolled                                        as de_1 --matches any year after they enroll
        on fs.school_district_id = de_1.district_id 
        and fs.school_year >= de_1.school_year_enrolled
    left join districts_enrolled                                        as de_2 --matches only the year they enroll
        on fs.school_district_id = de_2.district_id 
        and fs.school_year = de_2.school_year_enrolled
    left join districts_target                                        as dt
        on fs.school_district_id = dt.district_id 
        and fs.school_year = dt.school_year --assigns to most recent school year only
    order by
        fs.school_district_id
        , fs.school_year
)
select * 
from final