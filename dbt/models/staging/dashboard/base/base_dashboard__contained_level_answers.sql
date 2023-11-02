with 
source as (
    select * 
    from {{ source('dashboard', 'contained_level_answers') }}
),

renamed as (
    select
        id              as contained_level_answers_id,
        created_at,
        updated_at,
        level_id,
        answer_number,
        answer_text,
        correct         as is_correct
    from source
)

select * 
from renamed