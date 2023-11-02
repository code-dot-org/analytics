with 
source as (
<<<<<<< HEAD
      select * 
      from {{ source('dashboard', 'user_levels') }}
      where deleted_at is not null 
=======
    select * from {{ source('dashboard', 'user_levels') }}
    where deleted_at is null 
>>>>>>> main
),

renamed as (
    select
        id                          as user_level_id,
        user_id,
        level_id,
        script_id,
        level_source_id,
        attempts,
        created_at,
        updated_at,
        best_result,
        time_spent,
        submitted                   as is_submitted,
        readonly_answers            as is_read_only_answers,
<<<<<<< HEAD
        unlocked_at,
        properties
=======
        unlocked_at
        -- properties
>>>>>>> main
    from source
)

select * 
<<<<<<< HEAD
from renamed 
=======
from renamed
>>>>>>> main
