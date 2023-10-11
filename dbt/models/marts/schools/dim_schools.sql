-- Model: dim_schools
-- Scope: all dimensions we have/need for schools; one row per school + school_year
with
schools as (
    select *
    from {{ ref('stg_dashboard__schools') }}
),

school_districts as (
    select *
    from {{ ref('stg_dashboard__school_districts') }}
),

school_stats_by_years as (
    select *, row_number() over(partition by school_id order by school_year desc) as row_num
    from {{ ref('stg_dashboard__school_stats_by_years') }}
),

combined as (
    select
        -- schools
        schools.school_id,
        school_stats_by_years.school_year,
        schools.school_category,
        schools.school_name,
        schools.school_type,

        --school_districts
        school_districts.school_district_id,
        school_districts.school_district_name,

        -- dates 
        schools.last_known_school_year_open,
        min(schools.created_at) as school_created_at,
        max(schools.updated_at) as school_last_updated_at
    from schools
    left join school_stats_by_years
        on schools.school_id = school_stats_by_years.school_id
        and school_stats_by_years.row_num = 1
    left join school_districts
        on schools.school_district_id = school_districts.school_district_id
    {{ dbt_utils.group_by(8) }}
)

select *
from combined