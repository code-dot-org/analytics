with 
source as (
      select * from {{ source('pegasus_pii', 'form_geos') }}
),

renamed as (
    select
     {# {{ dbt_utils.star(
        from=ref('base_pegasus_pii__form_geos'),
            except=[
                "ip_address",
                "latitude",
                "longitude",
                "indexed_at"]) }} #}
        id as form_geo_id,
        form_id,
        created_at,
        updated_at,
        {# ip_address, #}
        city,
        state,
        country,
        postal_code
        {# latitude,
        longitude,
        indexed_at #}
    from source
)

select * from renamed
  