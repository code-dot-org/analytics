-- Model: dim_schools
-- Scope: all dimensions we have/need for schools; one row per school
with
schools as (
    select *
    from {{ ref('stg_dashboard__schools') }}
),

school_infos as (
    select *
    from {{ ref('stg_dashboard__school_infos') }}
),

school_districs as (
    select *
    from {{ ref('stg_dashboard__school_districts') }}
),

combined as (
    select
        -- schools
        schools.school_id,
        schools.school_category,

        --school_districts
        school_disticts.school_district_id,
        school_districts.school_district_name,

        -- school_info
        school_info.school_info_id,
        school_info.school_name,
        school_info.school_type,

        -- dates 
        min(schools.created_at) as school_created_at,
        max(schools.updated_at) as school_last_updated_at
    from schools
    left join school_info
        on schools.school_info_id = school_info.school_info_id
    left join school_districs
        on school.school_district_id = school_districts.school_district_id
    group by 1,2,3,4,5,6,7
    {# {{ dbt_utils.group_by('7') }} #}
)

select *
from combined
