with 
users as (
    select * 
    from {{ ref('base_dashboard__users') }}
    where is_active 
        and deleted_at is null 
),

renamed as (
    select 
        user_id,
        case when user_type = 'student' then user_id end as student_id,
        case when user_type = 'teacher' then user_id end as teacher_id,
        studio_person_id,
        sign_in_count,
        locale,
        datediff(year,birthday,current_date ) as age_years,
        user_type,
        school_info_id,
        total_lines,
        deleted_at,
        purged_at,
        nullif(lower(gender),'') as gender,
        is_urg,

        -- dates         
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at       
    from users 
)

select * 
from renamed 