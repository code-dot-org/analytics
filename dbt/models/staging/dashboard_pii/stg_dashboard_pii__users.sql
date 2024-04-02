with
users as (
    select *
    from {{ ref('base_dashboard_pii__users') }}
    where is_active
        and user_type is not null 
),

renamed as (
    select
        *, 
        case 
            when user_type = 'teacher' 
                then email 
            else null 
        end as teacher_email -- PII!
    from users
),

final as (
    select
        user_id,
        user_type,
        
        -- email,
        teacher_email, -- PII!
        
        gender,
        birthday,
        is_active,
        is_urg,
        races,
        created_at,
        updated_at,
        purged_at,
        deleted_at
    from renamed )

select *
from final