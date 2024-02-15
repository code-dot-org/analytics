with 
contained_level_answers as (
    select 
        level_id,
        answer_number,
        answer_text,
        is_correct
    from {{ ref('base_dashboard__contained_level_answers') }} )

select * 
from contained_level_answers

