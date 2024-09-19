-- model: dim_user_projects

with projects as (
    select 
        project_id,
        storage_id,
        project_type,
        created_at:: date as activity_date
    from {{ ref('stg_dashboard_pii__projects')}}
),

user_project_storage as (
    select 
        user_id,
        user_project_storage_id
    from {{ ref('stg_dashboard__user_project_storage_ids') }}
),

users as (
    select * 
    from {{ ref('dim_users') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select *,
        
        case 
            when ups.user_id is not null 
            then 1 else 0 end as known_cdo_user,

        coalesce(
            ups.user_id::varchar, 
            'storage_id_' || proj.storage_id::varchar
        ) as user_id_merged

    from projects as proj 
    left join user_project_storage as ups 
        on proj.project_id = ups.user_project_storage_id
),

final as (
    select 
        -- user info
        comb.user_id,
        comb.user_id_merged,
        comb.known_cdo_user,
        users.country,
        users.us_intl,

        -- projects 
        comb.project_id,
        comb.storage_id,
        comb.user_project_storage_id,
        project_type,
        
        -- dates
        sy.school_year,
        comb.activity_date
        
    from combined as comb
    join users 
        on comb.user_id = users.user_id 
    join school_years as sy 
        on comb.activity_date 
            between sy.started_at 
            and sy.ended_at )
    

select * 
from final 
-- where activity_date > '2024-01-01' 
-- order by user_id, activity_date 