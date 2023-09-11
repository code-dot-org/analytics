with 
source as (
      select * from "dashboard"."dashboard_production"."user_scripts"
),

renamed as (
    select
        id as user_script_id,
        user_id,
        script_id,
        started_at,
        completed_at,
        assigned_at,
        last_progress_at,
        created_at,
        updated_at,
        properties,
        deleted_at
    from source
)

select * from renamed