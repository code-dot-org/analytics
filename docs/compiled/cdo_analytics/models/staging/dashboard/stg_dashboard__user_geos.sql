with 
 __dbt__cte__base_dashboard__user_geos as (
with 
source as (
      select * from "dashboard"."dashboard_production"."user_geos"
),

renamed as (
    select
        id as user_geo_id,
        user_id,
        created_at,
        updated_at,
        indexed_at,
        city,
        state,
        country,
        postal_code
    from source
)

select * from renamed
), user_geos as (
    select *,
        case when lower(country) = 'united states' then 1 
             when lower(country) <> 'united states' then 0 
             else null 
        end as is_international
    from __dbt__cte__base_dashboard__user_geos
)

select * from user_geos