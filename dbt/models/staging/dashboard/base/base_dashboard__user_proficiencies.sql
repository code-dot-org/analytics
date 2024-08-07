with 
source as (
    select * 
    from {{ source('dashboard', 'user_proficiencies') }}
),

renamed as (
    select
        id as user_proficiency_id,
        user_id,
        last_progress_at,
        sequencing_d1_count,
        sequencing_d2_count,
        sequencing_d3_count,
        sequencing_d4_count,
        sequencing_d5_count,
        debugging_d1_count,
        debugging_d2_count,
        debugging_d3_count,
        debugging_d4_count,
        debugging_d5_count,
        repeat_loops_d1_count,
        repeat_loops_d2_count,
        repeat_loops_d3_count,
        repeat_loops_d4_count,
        repeat_loops_d5_count,
        repeat_until_while_d1_count,
        repeat_until_while_d2_count,
        repeat_until_while_d3_count,
        repeat_until_while_d4_count,
        repeat_until_while_d5_count,
        for_loops_d1_count,
        for_loops_d2_count,
        for_loops_d3_count,
        for_loops_d4_count,
        for_loops_d5_count,
        events_d1_count,
        events_d2_count,
        events_d3_count,
        events_d4_count,
        events_d5_count,
        variables_d1_count,
        variables_d2_count,
        variables_d3_count,
        variables_d4_count,
        variables_d5_count,
        functions_d1_count,
        functions_d2_count,
        functions_d3_count,
        functions_d4_count,
        functions_d5_count,
        functions_with_params_d1_count,
        functions_with_params_d2_count,
        functions_with_params_d3_count,
        functions_with_params_d4_count,
        functions_with_params_d5_count,
        conditionals_d1_count,
        conditionals_d2_count,
        conditionals_d3_count,
        conditionals_d4_count,
        conditionals_d5_count,
        basic_proficiency_at,
        created_at,
        updated_at
    from source
)

select * 
from renamed