{{
    config(
        materialized='incremental',
        unique_key='user_level_id'
    )
}}

with 
user_levels as (
    select 
        *,
        json_extract_path_text(properties, 'locale', true) as selected_locale,
        case 
            when json_extract_path_text(properties, 'locale_supported', true) = 'true' then 1
            when json_extract_path_text(properties, 'locale_supported', true) = 'false' then 0
            else null
        end as is_locale_supported
    from {{ ref('base_dashboard__user_levels') }}

    {% if is_incremental() %}

    where updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
)

select * 
from user_levels