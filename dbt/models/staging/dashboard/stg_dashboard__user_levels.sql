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

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
    where coalesce(created_at,updated_at) > (select max(coalesce(created_at,updated_at)) from {{ this }} ) -- only new records

    {% endif %}
)

select * 
from user_levels