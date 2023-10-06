with 
users as (
    select {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=[
            "admin",
            "birthday",
            "primary_contact_info_id"]) }}
    from {{ ref('stg_dashboard__users')}}
    where purged_at is null
),

user_geos as (
    select 
        user_id,
        is_international,
        country
    from {{ ref('stg_dashboard__user_geos')}}
    where user_id in (select user_id from users)
),

combined as (
    select
        --user info
        users.user_id,
        student_id,
        teacher_id,
        studio_person_id,
        user_type,
        gender,
        age_years,
        is_urg,

        -- user_geo
        is_international,
        country,

        -- misc
        locale,
        sign_in_count,
        -- school_info_id,
        total_lines,

        -- dates         
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at  
        purged_at,
        deleted_at
    from users 
    join user_geos 
        on users.user_id = user_geos.user_id 
)

select * 
from combined 