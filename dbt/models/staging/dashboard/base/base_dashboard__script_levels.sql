with 
source as (
      select * from {{ source('dashboard', 'script_levels') }}
),

renamed as (
    select
        id                          as script_level_id,
        script_id,
        chapter,
        created_at,
        updated_at,
        stage_id,
        position,
        assessment                  as is_assessment,
        properties,
        named_level                 as is_named_level,
        bonus                       as is_bonus,
        activity_section_id,
        seed_key,
        activity_section_position
    from source
)

select * from renamed