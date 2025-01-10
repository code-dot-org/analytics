with 
forms as (
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
        location_country_code,
        -- form_data_text,  
        -- processed_data_text,
        lower(nullif(json_extract_path_text(processed_data_text, 'location_city_s', true), ''))      as city,
        lower(nullif(json_extract_path_text(processed_data_text, 'location_state_s', true), ''))     as state,
        lower(nullif(json_extract_path_text(processed_data_text, 'location_country_s', true), ''))   as country,
        lower(nullif(json_extract_path_text(form_data_text, 'event_type_s', true), ''))              as event_type,
        nullif(json_extract_path_text(form_data_text, 'email_preference_opt_in_s', true), '')        as email_pref,
        nullif(json_extract_path_text(form_data_text, 'special_event_flag_b', true), '')             as special_event_flag
    from {{ ref('base_pegasus_pii__forms') }}
)

select
    form_id,
    form_category,
    hoc_year,
    email,
    name,
    form_kind,
    created_at,
    updated_at,
    indexed_at,
    review,
    reviewed_by,
    reviewed_at,
    processed_at,
    notified_at,
    user_id,
    parent_id,
    location_country_code,
    city,
    state,
    {{ country_normalization('country')}} as country,
    event_type,
    email_pref,
    special_event_flag
from forms  

