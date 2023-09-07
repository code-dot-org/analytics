with 
source as (
      select * from {{ source('dashboard_pii', 'user_school_infos') }}
),

renamed as (
    select
        id                      as user_school_info_id,
        user_id,
        start_date              as started_at,
        end_date                as ended_at,
        school_info_id,
        last_confirmation_date  as last_confirmation_at,
        created_at,
        updated_at
    from source
)

select * from renamed