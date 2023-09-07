with 
source as (
      select * from {{ source('dashboard', 'sign_ins') }}
),

renamed as (
    select
        id as sign_in_id,
        user_id,
        sign_in_at,
        sign_in_count
    from source
)

select * from renamed