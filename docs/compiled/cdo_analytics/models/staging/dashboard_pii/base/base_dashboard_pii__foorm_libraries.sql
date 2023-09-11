with 
source as (
      select * from "dashboard"."dashboard_production_pii"."foorm_libraries"
),

renamed as (
    select
        id          as foorm_library_id,
        name,
        version,
        published   as is_published,
        created_at,
        updated_at
    from source
)

select * from renamed