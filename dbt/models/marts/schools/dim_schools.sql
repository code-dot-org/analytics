-- Model: dim_schools
-- Scope: all dimensions we have/need for schools; one row per school
with
schools as (
    select *
    from {{ ref('stg_dashboard__schools') }}
),

school_info as (
    select *
    from {{ ref('stg_dashboard__school_infos') }}
),

school_districts as (
    select *
    from {{ ref('stg_dashboard__school_districts') }}
),

school_stats_by_years as (
    select *
    from {{ ref('stg_dashboard__school_stats_by_years') }}
),

combined as (
    select
        -- schools
        schools.school_id,
        school_stats_by_years.school_year,
        schools.school_category,

        --school_districts
        school_districts.school_district_id,
        school_districts.school_district_name,

        -- school_info
        school_info.school_info_id,
        school_info.school_name,
        school_info.school_type,

        -- dates 
        schools.last_known_school_year_open,
        min(schools.created_at) as school_created_at,
        max(schools.updated_at) as school_last_updated_at
    from schools
    left join school_stats_by_years
        on schools.school_id = school_stats_by_years.school_id
    left join school_info
        on schools.school_id = school_info.school_id
    left join school_districts
        on schools.school_district_id = school_districts.school_district_id
    group by 1,2,3,4,5,6,7,8,9
)

select *
from combined
