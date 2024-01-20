

with 

dim_schools as (
    select * 
    from "dev"."dbt_jordan"."dim_schools"
),

all_districts as (
    select school_district_id
    from "dev"."dbt_jordan"."dim_districts"
),

school_years as (
    select * 
    from "dev"."dbt_jordan"."int_school_years"
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
    from "dev"."dbt_jordan"."int_teacher_schools_historical"
),

teacher_active_courses as (
    select 
        distinct teacher_id,
        school_year,
        course_name,
        section_started_at
    from "dev"."dbt_jordan"."int_active_sections"
),

teacher_active_courses_with_sy as (

    select
        tac.teacher_id,
        tac.school_year,
        tac.course_name,
        tac.section_started_at,
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

started_districts as (
    select 
        school_district_id,
        school_year,
        min(section_started_at) as district_started_at,
        listagg( distinct course_name, ', ') within group (order by course_name) active_courses
    from teacher_active_courses_with_sy
    group by 1, 2
),

active_status_simple as (
    select 
        all_districts_sy.school_district_id,
        all_districts_sy.school_year,
        case when started_districts.school_district_id is null then 0 else 1 end as is_active,
        started_districts.district_started_at,
        started_districts.active_courses
    from all_districts_sy 
    left join started_districts
        on started_districts.school_district_id = all_districts_sy.school_district_id 
        and started_districts.school_year = all_districts_sy.school_year
),

full_status as (
    -- Determine the active status for each school in each school year

    select
        school_district_id,
        school_year,
        is_active,
        coalesce(
            lag(is_active, 1) 
                over (partition by school_district_id order by school_year) 
            , 0
        ) as prev_year_active,
        coalesce( --force any NULL to be 0 for this function
            max(is_active) 
                over (partition by school_district_id order by school_year rows between unbounded preceding and 1 preceding)
            , 0
        ) as ever_active_before,
        (is_active || prev_year_active || ever_active_before) status_code,
        district_started_at,
        active_courses
    from
        active_status_simple

), 

final as (

    select
        school_district_id,
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
        district_started_at,
        active_courses
        from full_status
    order by
        school_district_id, school_year
)

select * 
from final