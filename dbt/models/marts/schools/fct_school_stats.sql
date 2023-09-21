with 
schools as (
    select * 
    from {{ ref('stg_dashboard__schools') }}
),

school_districts as (
    select * 
    from {{ ref('stg_dashboard__school_districts')}}
),

school_stats_by_years as (
    select * 
    from {{ ref('stg_dashboard__school_stats_by_years') }}
),

school_stats_19_20 as (
    select *
    from school_stats_by_years
    where school_year = '2019-2020'
),

frl_eligibility_status as (         
    -- (js) this model should probably be migrated to dim_schools
    -- status for SY 20-21
    select ssby.school_id, 
        case 
            when coalesce(ssby2.total_frl_eligible,ssby.total_frl_eligible) is not null 
            then 1 else 0 end                                           as is_frl_eligible,
            coalesce(ssby2.total_frl_eligible,ssby.total_frl_eligible)  as total_frl_eligible,
    from school_stats_by_years as ssby
    inner join school_stats_19_20 as ssby2
        on ssby.school_id = ssby2.school_id 
    left join survey_years as sy 
        on ssby.school_id = sy.school_id
    where survey_year.school_year = '2020-2021'
),


aggregated as (
    select 
        school_id,
        coalesce(total_frl_eligible,0) / coalesce(total_students) as pct_frl_eligible,
        case when pct_frl_eligible > 0.5 then 1 else 0 end as is_high_needs        
    from eligibility_status
),

combined as (
    select 
        -- schools
        schools.school_id, 
        schools.school_district_id,

        --school_district
        school_districts.school_district_name,


        -- school_stats
        {{ dbt_utils.star(
            from=ref('school_stats_by_years'),
            except=["school_id","created_at","updated_at"]) }},

        -- scholarships
        frles.pct_frl_eligible,
        frles.is_high_needs
    from schools
    left join school_info 
        on schools.school_info_id = school_info.school_info_id
    left join school_districts 
        on schools.school_district_id = school_districts.school_district_id
    left join school_stats_by_years as ssby 
        on schools.school_id = ssby.school_id 
    left join frl_eligibility_status as frles 
        on schools.school_id = frles.school_id 
)

select * 
from combined