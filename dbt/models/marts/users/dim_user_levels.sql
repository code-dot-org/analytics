-- fka: int_user_levels
-- scope: capture user_level data in one model

with 
user_levels as (
    select 
        user_id,
        level_id,
        script_id,
        level_source_id,
        is_submitted,
        
        to_date(unlocked_at,'yyyymmdd') as unlocked_date,
        to_date(created_at, 'yyyymmdd') as created_date,
        to_date(updated_at, 'yyyymmdd') as updated_date,
        
        sum(attempts)       as num_attempts,
        max(best_result)    as best_result,
        sum(time_spent)     as time_spent    

    from {{ ref('stg_dashboard__user_levels') }}
    {{ dbt_utils.group_by(8) }}
)

select * from user_levels 

users as (
    select *
    from {{ ref('dim_users') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years')}}
),


combined as (
    select 
        coalesce(
            usl.created_date,
            usl.updated_date) as activity_date,
        
        usl.unlocked_at,
        usl.level_id,
        usl.script_id,
        usr.us_state,
        usr.country,
        usr.us_intl,
        usr.is_international,
        usl.num_attempts,
        usl.best_result,
        usl.time_spent,

        count(distinct usl.user_id) as num_users

    from user_levels    as usl 
    join users          as usr
        on usl.user_id = usr.user_id
    
    join school_years   as sy 
        on usl.created_date
            between sy.start_date
                and sy.end_date

    where sy.school_year = '2023-24'

    {{ dbt_utils.group_by(10) }}
)

select * 
from combined 

final as (
    select 
        activity_date,
        level_id,
        script_id,
        us_state,
        country,
        us_intl,
        num_attempts,
        best_result,
        time_spent,
        num_users
    from combined )

select * 
from final