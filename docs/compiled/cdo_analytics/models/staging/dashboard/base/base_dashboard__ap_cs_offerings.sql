with 
source as (
      select * from "dashboard"."dashboard_production"."ap_cs_offerings"
),

renamed as (
    select
        id as ap_cs_offerings_id,
        school_code,
        course,
        school_year,
        created_at,
        updated_at
    from source
)

select * from renamed