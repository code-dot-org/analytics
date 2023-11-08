{# Notes:
Design: 1 row per teacher, school_year, churn_status
Logic: where teacher has active section, then:
    -- active retained: had an active section last SY and has an active section this SY (could be the same section ID, but needs to have 5+ active students both SYs)
    -- active reacquired: did not have an active section last SY, but does have one this SY
    -- active new: never has had an active section before, but has one this SY
    -- inactive churn: did not have an active section last SY and does not have an active section this SY
    -- inactive this year: had an active section last SY, does not have one this SY
    -- market: has a teacher account but has never had an active section 
#}

with 
active_teachers as (
    select teacher_id,
        school_year,
        1 as is_active
    from {{ ref('int_active_sections') }}
    where teacher_id is not null 
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        active_teachers.teacher_id,
        school_years.school_year,
        case when active_teachers.school_year is not null then 1 else 0 end as is_active 
    from active_teachers 
    left join school_years 
        on active_teachers.school_year = school_years.school_year
),

augmented as (
    select 
        teacher_id,
        school_year,
        is_active,
        lag(is_active,1) over(order by school_year) is_active_previous_year
    from combined
),

aggregated as (
    select teacher_id, 
        min(school_year) as first_year_active,
        max(school_year) as last_year_active,
        sum(is_active) as total_years_active
    from augmented
    {{ dbt_utils.group_by(1) }}
),

final as (
    select 
        augmented.teacher_id,
        augmented.school_year,
        -- churn status
        case when is_active and is_active_previous_year     then 'active - retained'
             when is_active and not is_active_previous_year then 'active - reacquired'
             when is_active and total_years_active = 1      then 'active - new'
             when not is_active and not is_active_previous_year then 'inactive - churn'
             when not is_active and is_active_previous_year then 'inactive - this year'
             when total_years_active = 0 then 'market' else 'n/a' end as churn_status
    from augmented
    join aggregated
        on augmented.teacher_id = aggregated.teacher_id
)

select *
from final 