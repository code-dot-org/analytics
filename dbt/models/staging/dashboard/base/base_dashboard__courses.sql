with 
source as (
      select * from {{ source('dashboard', 'courses') }}
),

renamed as (
    select
        id as course_id,
        name,
        properties,
        created_at,
        updated_at
    from source
)

select * from renamed