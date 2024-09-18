with 
sign_ins as (
    select *
    from {{ ref('stg_dashboard__sign_ins') }}
),

users as (
    select * 
    from {{ ref('dim_users') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

final as (
    select 
        -- user info
        sii.user_id, 
        usr.user_type,
        usr.country,
        usr.us_intl,
        
        -- dates 
        sy.school_year,
        sii.sign_in_at::date as activity_date,
        
        -- aggs 
        sum(sii.sign_in_count) as num_sign_ins  
        
    from sign_ins   as sii 
    left join users as usr 
        on sii.user_id = usr.user_id
    
    join school_years as sy 
        on sii.sign_in_at::date  
            between sy.started_at 
                and sy.ended_at 
    {{ dbt_utils.group_by(6) }} )

select * 
from final 