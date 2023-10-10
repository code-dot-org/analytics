with 
users as (
    select *
    from {{ ref('base_dashboard__users') }}
    where is_active 
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
        purged_at
    from users
)

select * 
from renamed