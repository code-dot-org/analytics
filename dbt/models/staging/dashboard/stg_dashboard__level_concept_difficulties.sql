with 
level_concept_difficulties as (
    select * 
    from {{ ref('base_dashboard__level_concept_difficulties') }}
),

final as (
    select 
        -- level_concept_difficulty_id,
        level_id,
        
        sequencing,
        debugging,
        repeat_loops,
        repeat_until_while,
        for_loops,
        events,
        variables,
        functions,
        functions_with_params,
        conditionals,

        created_at,
        updated_at
    from level_concept_difficulties
)

select *
from final