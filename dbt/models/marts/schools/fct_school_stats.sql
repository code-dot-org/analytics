with 

schools as (
    select * 
    from {{ ref('stg_dashboard__schools') }}
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

frl_eligibility_status as (         -- (js) this model should probably be migrated to dim_schools
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
school_districts as (
    select * 
    from {{ ref('stg_dashboard__school_districts')}}
),

survey_years as (
    select 
        school_id,
        max(school_year) as survey_year,
        min(school_year) as first_survey_year
    from school_stats_by_years
    group by school_id
),

-- (js) still missing here: teacher_trainings data 

aggregated as (
    select school_id,
        coalesce(total_frl_eligible,0) / coalesce(total_students) as pct_frl_eligible,
        case when pct_frl_eligible > 0.5 then 1 else 0 end as is_high_needs        
    from eligibility_status
),

combined as (
    select 
        school_id, 
        
    from schools -- (js) eventually dim_schools

)