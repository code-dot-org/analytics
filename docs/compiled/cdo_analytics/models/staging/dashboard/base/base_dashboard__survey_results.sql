with 
source as (
      select * from "dashboard"."dashboard_production"."survey_results"
),

renamed as (
    select
        id as survey_result_id,
        user_id,
        kind,
        properties,
        created_at,
        updated_at

    from source
)

select * from renamed