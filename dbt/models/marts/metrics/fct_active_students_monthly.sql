{# 
    model: fct_active_students_monthly
    changelog:
    author      version date        comments
    js          2.0    2024-09-17   init 
    ""          2.1     ""          removing anonymous users from scope
#}

with 
users as (
    select * 
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

user_levels as (
    select  

        user_id             as user_id,
        created_date        as activity_date,
        'u'             as activity_type,
        1               as is_active,
        1               as has_user_level_activity,
        null            as has_sign_in_activity,
        null            as has_project_activity
        
        -- user_type,
        -- known_cdo_user, -- only capturing student; no anonymous (yet)    (js; 20240920)
        

    from {{ ref('dim_user_levels') }}
    where 
        created_date > {{ get_cutoff_date() }}
        and total_attempts > 0 
), 

sign_ins as (
    select 

        user_id,
        sign_in_date        as activity_date,
        's'                 as activity_type,
        1                   as is_active, 
        null                as has_user_level_activity,
        1                   as has_sign_in_activity,
        null                as has_project_activity

    from {{ ref('dim_user_sign_ins') }}
    where 
        sign_in_date > {{ get_cutoff_date() }}
        and num_sign_ins > 0
), 

projects as (
    select 

        user_id,
        project_created_at::date    as activity_date,
        1                           as is_active,
        'p'                         as activity_type,
        null                        as has_user_level_activity,
        null                        as has_sign_in_activity,
        1                           as has_project_activity

    from {{ ref('dim_student_projects') }}
    where project_created_at > {{ get_cutoff_date() }}
        and user_type = 'student'
),

school_years as (
    select * from {{ ref("int_school_years") }} 
    where started_at > {{ get_cutoff_date() }}
),

combined as (
    select * from user_levels  
    union all 
    select * from sign_ins 
    union all 
    select * from projects 
),

final as (
    select 
        date_trunc('month', comb.activity_date) as activity_month,
        sy.school_year,
        usr.us_intl,
        usr.country,


        count(distinct 
            case 

            {#  Active Student Metric: 
            
            1.  Any user_id with a sign_in on any given day 
                
                AND
                
            2.a. an attempted user_level activity 
                OR
            
            2.b. A `projects` row exists 

            -- js; 20240920                         #}
        
        {#  case when sum(comb.known_cdo_user) >= 1 
             then max(usr.user_type) 
             else 'anon' end                        as user_type_merged, #}

                when comb.has_sign_in_activity  = 1
                and coalesce(
                    comb.has_user_level_activity,
                    comb.has_project_activity)   = 1
            then comb.user_id end)                                    as num_active_students

        from combined   as comb 
        join users      as usr
            on comb.user_id = usr.user_id 
            
        join school_years as sy 
            on comb.activity_date 
                between sy.started_at 
                    and sy.ended_at

        {{ dbt_utils.group_by(4) }} )

select *
from final
order by 
    activity_month desc 