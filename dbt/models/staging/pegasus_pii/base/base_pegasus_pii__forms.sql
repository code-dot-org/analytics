with 
source as (
    select * 
    from {{ source('pegasus_pii', 'forms') }}
),

renamed as (
    select
        id                      as form_id,
        source_id,
        {# secret, #}
        email,
        name,
        kind                    as form_kind,
        -- data                    as form_data,
        created_at,
        {# created_ip, #}
        updated_at,
        {# updated_ip, #}
        indexed_at,
        review,
        reviewed_by,
        reviewed_at,
        {# reviewed_ip, #}
        processed_at,
        -- processed_data, 
        notified_at,
        user_id,
        parent_id,
        {# hashed_email, #}
        location_country_code_s as location_country_code,
        data_text               as form_data_text,  
        processed_data_text
    from source
)

select * 
from renamed
  