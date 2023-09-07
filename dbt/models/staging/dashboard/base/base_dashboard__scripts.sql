with 
source as (
      select * from {{ source('dashboard', 'scripts') }}
),

renamed as (
    select
        id as script_id,
        name as script_name,
        created_at,
        updated_at,
        wrapup_video_id,
        user_id,
        login_required,
        properties,
        new_name,
        family_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience
    from source
)

select * from renamed