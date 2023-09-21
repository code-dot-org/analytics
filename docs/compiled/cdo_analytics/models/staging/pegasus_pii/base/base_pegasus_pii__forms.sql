with 
source as (
      select * from "dashboard"."pegasus_pii"."forms"
),

renamed as (
    select
        id                      as form_id,
        source_id,
        
        email,
        name,
        kind                    as form_kind,
        data                    as form_data,
        created_at,
        
        updated_at,
        
        indexed_at,
        review,
        reviewed_by,
        reviewed_at,
        
        processed_at,
        processed_data, 
        notified_at,
        user_id,
        parent_id,
        
        location_country_code_s as location_country_code,
        data_text               as form_data_text,  
        processed_data_text
    from source
)

select * from renamed