with 
 __dbt__cte__base_dashboard__school_infos as (
with 
source as (
      select * from "dashboard"."dashboard_production"."school_infos"
),

renamed as (
    select
        id as school_info_id,
        country,
        school_type,
        zip,
        state,
        school_district_id,
        school_district_other,
        school_district_name,
        school_id,
        school_other,
        school_name,
        full_address,
        created_at,
        updated_at,
        validation_type
    from source
)

select * from renamed
), school_infos as (
    select * from __dbt__cte__base_dashboard__school_infos
)

select * from school_infos