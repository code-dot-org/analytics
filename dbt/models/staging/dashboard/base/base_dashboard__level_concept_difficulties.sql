with source as (
      select * from {{ source('dashboard', 'level_concept_difficulties') }}
),

renamed as (
    select
        id as level_concept_difficulty_id,
        level_id,
        created_at,
        updated_at,
        sequencing,
        debugging,
        repeat_loops,
        repeat_until_while,
        for_loops,
        events,
        variables,
        functions,
        functions_with_params,
        conditionals
    from source
)

select * from renamed 