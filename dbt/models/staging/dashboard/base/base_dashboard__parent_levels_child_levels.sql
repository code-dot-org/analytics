with 
source as (
      select * from {{ source('dashboard', 'parent_levels_child_levels') }}
),

renamed as (
    select
        id as parent_levels_child_level_id,
        parent_level_id,
        child_level_id,
        position,
        kind
    from source
)

select * from renamed