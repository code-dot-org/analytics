with 
source as (
      select * from {{ source('dashboard_pii', 'foorm_forms') }}
),

renamed as (
    select
        id          as foorm_foorm_id,
        name,
        version,
        questions,
        created_at,
        updated_at,
        published   as is_published
    from source
)

select * from renamed