with 
source as (
    select * 
    from {{ source('dashboard', 'followers') }}
    
),

renamed as (
    select
        id                  as follower_id,
        student_user_id     as student_id,
        section_id,
        created_at          as student_added_at,
        deleted_at          as student_removed_at,
        updated_at
    from source
)

select * 
from renamed