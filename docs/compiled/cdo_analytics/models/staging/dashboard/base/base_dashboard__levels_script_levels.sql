with 
source as (
      select * from "dashboard"."dashboard_production"."levels_script_levels"
),

renamed as (
    select
        level_id,
        script_level_id
    from source
)

select * from renamed