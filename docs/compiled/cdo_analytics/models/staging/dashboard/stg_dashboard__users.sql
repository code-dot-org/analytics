

with 
 __dbt__cte__base_dashboard__users as (
with 
source as (
      select * 
      from "dashboard"."dashboard_production"."users"
      --where deleted_at is null 
),

renamed as (
    select
        id                          as user_id,
        studio_person_id,
        sign_in_count,
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,
        deleted_at,        
        gender,
        locale,
        birthday,
        user_type,
        school_info_id,
        total_lines,
        active                      as is_active,
        purged_at,
        urm                         as is_urg,
        races,
        primary_contact_info_id
    from source
)

select * 
from renamed
), users as (
    select *
    from __dbt__cte__base_dashboard__users
    where is_active

    

    and updated_at > (select max(updated_at) from "dev"."dbt_jordan"."stg_dashboard__users" )
    
    
),

renamed as (
    select 
        -- PK
        user_id,

        -- FK's
        case when user_type = 'student' then user_id end as student_id,
        case when user_type = 'teacher' then user_id end as teacher_id,
        studio_person_id,

        -- user info
        user_type,
        datediff(year,birthday,current_date ) as age_years,
        nullif(lower(gender),'') as gender,
        is_urg,

        -- misc.
        locale,
        sign_in_count,
        school_info_id,
        total_lines,

        -- dates         
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,  
        deleted_at,   
        purged_at
    from users
)

select * 
from renamed