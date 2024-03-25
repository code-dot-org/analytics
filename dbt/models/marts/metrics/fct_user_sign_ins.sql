{# 
    model: metric_user_sign_ins
    desc: combining existing metrics models into one for more
    accurate and readable reporting.

#}

{# 
    {{
    
  -- Option to load incrementally:
  config(
    materialized = 'incremental',
    unique_key = 
        ('user_id', 
        'sign_in_at')
  )
}} 

-- js 2024-03-22

#}

with 
school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

sign_ins as (
    select *
    from {{ ref('stg_dashboard__sign_ins')}}
),

users as (
    select 
        user_id,
        user_type,
        us_intl,
        country
    from {{ ref('dim_users') }}
    where current_sign_in_at is not null 
),

combined as (
    select 
        -- user info 
        si.user_id,
        us.user_type,

        us.us_intl,
        us.country,
        sy.school_year,
        
        si.sign_in_at,
        extract(month   from si.sign_in_at)     as sign_in_month,
        extract(year    from si.sign_in_at)     as sign_in_year


    from sign_ins   as si 

    left join users as us 
        on si.user_id = us.user_id
    
    left join school_years as sy
        on si.sign_in_at 
            between sy.started_at 
                and sy.ended_at
),

aggregated as (
    select 
        school_year                 as "School Year",
        'monthly'                   as "Report Type",   
        sign_in_month               as "Report Date",
        user_type                   as "User Type",     
        us_intl                     as "US / International",
        country                     as "Country",
        count(distinct user_id)     as "Number of Sign Ins"
    from combined
    {{ dbt_utils.group_by(6) }}

    union all 

    select 
        school_year,
        'annual',
        sign_in_year,
        user_type,
        us_intl,
        country,
        count(distinct student_id),
        count(distinct teacher_id)
    from combined
    {{ dbt_utils.group_by(6) }}
),

final as (
    select *
    from aggregated 
    order by 
        "School Year", 
        "Report Type", 
        "Report Date")

select * 
from final 
