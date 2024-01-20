with 
 __dbt__cte__base_pegasus_pii__forms as (
with 
source as (
    select * 
    from "dashboard"."pegasus_pii"."forms"
),

renamed as (
    select
        id                      as form_id,
        source_id,
        email,
        name,
        kind                    as form_kind,
        -- data                    as form_data,
        created_at,
        updated_at,
        indexed_at,
        review,
        reviewed_by,
        reviewed_at,
        processed_at,
        -- processed_data, 
        notified_at,
        user_id,
        parent_id,
        location_country_code_s as location_country_code,
        data_text               as form_data_text,  
        processed_data_text
    from source
)

select * 
from renamed
), forms as (
    select 
        form_id,
        case
            when lower(form_kind) like 'hocsignup%' then 'hoc'
            when lower(form_kind) = 'professionaldevelopmentworkshop' then 'pd'
            else lower(form_kind)
        end                                                             as form_category,
        case 
            when lower(form_kind) like 'hocsignup%' 
            then right(form_kind,4) 
            else null 
        end                                                             as hoc_year,
        email,
        name,
        form_kind,
        -- form_data,
        created_at,
        updated_at,
        indexed_at,
        review,
        reviewed_by,
        reviewed_at,
        processed_at,
        -- processed_data, 
        notified_at,
        user_id,
        parent_id,
        location_country_code,
        -- form_data_text,  
        -- processed_data_text,
        nullif(json_extract_path_text(processed_data_text, 'location_city_s', true), '')      as city,
        nullif(json_extract_path_text(processed_data_text, 'location_state_s', true), '')     as state,
        nullif(json_extract_path_text(processed_data_text, 'location_country_s', true), '')   as country,
        nullif(json_extract_path_text(form_data_text, 'event_type_s', true), '')              as event_type,
        nullif(json_extract_path_text(form_data_text, 'email_preference_opt_in_s', true), '') as email_pref
    from __dbt__cte__base_pegasus_pii__forms
)

select * 
from forms