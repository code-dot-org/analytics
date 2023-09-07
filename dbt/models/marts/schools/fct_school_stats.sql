with schools as (
    select * 
    from {{ ref('dim_schools') }}
),

school_stats as (
    select * 
    from {{ ref('stg_dashboard__school_stats_by_years') }}
),

school_districts as (
    select * 
    from {{ ref('stg_dashboard__school_districts')}}
),

previous_year_school_stats as (
    select *
    from school_stats
    where school_year = '2019-2020'
),

combined as (
    select 
        ss.survey_year, 
        ss.first_survey_year, 
        ss.school_id,

        case when ss.survey_year = '2020-2021'
            then coalesce(pyss.total_frl_eligible,ss.total_frl_eligible) 
            else null end as total_frl_eligible_calculated,


        case when ss.survey_year = '2020-2021'
                then coalesce(
                    nullif(pyss.total_frl_eligible / pyss.total_students::float,0),
                    nullif(ss.total_frl_eligible / ss.total_students::float,0))
                else null 
        end as pct_frl_eligible,       

        case when coalesce(
                nullif(pyss.total_frl_eligible / pyss.total_students::float,0),
                nullif(ss.total_frl_eligible / ss.total_students::float,0))> 0.5
            then 1 else null 
        end as is_high_needs

    from school_stats as ss 
    left join previous_year_school_stats as pyss 
        on ss.school_id = pyss.school_id

)

select * 
from combined

