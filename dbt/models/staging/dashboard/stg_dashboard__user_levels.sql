{{
    config(
        materialized='incremental',
        unique_key='user_level_id'
    )
}}

with 
user_levels as (
    select * 
    from {{ ref('base_dashboard__user_levels') }}

    {% if is_incremental() %}

    where updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
)

select * 
from user_levels