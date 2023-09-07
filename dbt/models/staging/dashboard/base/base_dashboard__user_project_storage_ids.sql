with 
source as (
      select * from {{ source('dashboard', 'user_project_storage_ids') }}
),

renamed as (
    select
        id as user_project_storage_id,
        user_id
    from source
)

select * from renamed