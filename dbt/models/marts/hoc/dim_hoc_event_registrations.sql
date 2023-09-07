with 
forms as (
    select 
        form_id,
        right(form_kind,4) as hoc_year,
        email,
        name,
        form_kind,
        form_data,
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
        location_country_code,
        form_data_text,  
        processed_data_text,
        city,
        state,
        country,
        registered_at,
        last_updated_at
    from {{ ref('dim_forms') }}
    where lower(form_kind) like 'hocsignup%'
)

select * 
from forms

