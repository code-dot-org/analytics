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
        json_extract_path_text(properties, 'locale', true) as selected_language,
        json_extract_path_text(properties, 'locale_supported', true) as is_language_supported
    from {{ ref('base_dashboard__user_levels') }}

    {% if is_incremental() %}

    where updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
)

select * 
from user_levels