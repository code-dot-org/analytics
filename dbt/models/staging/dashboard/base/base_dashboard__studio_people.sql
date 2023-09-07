with 
source as (
      select * from {{ source('dashboard', 'studio_people') }}
),

renamed as (
    select
        id as studio_people_id,
        created_at,
        updated_at
    from source
)

select * from renamed