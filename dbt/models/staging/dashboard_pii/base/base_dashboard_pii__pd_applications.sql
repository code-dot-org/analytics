with 
source as (
      select * from {{ source('dashboard_pii', 'pd_applications') }}
),

renamed as (
    select
        id  as pd_application_id,
        user_id,
        type,
        application_year,
        application_type,
        regional_partner_id,
        status,
        locked_at,
        notes,
        form_data,
        created_at,
        updated_at,
        course,
        response_scores,
        application_guid,
        accepted_at,
        properties,
        deleted_at,
        status_timestamp_change_log,
        applied_at
    from source
)

select * from renamed