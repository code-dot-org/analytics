with 
source as (
      select * from {{ source('dashboard_pii', 'projects') }}
),

renamed as (
    select
        id                      as project_id,
        storage_id,
        value,
        updated_at,
        updated_ip,
        state,
        created_at,
        abuse_score,
        project_type,
        published_at,
        standalone              as is_standalone,
        remix_parent_id,
        skip_content_moderation as is_skip_content_moderation
    from source
)

select * from renamed