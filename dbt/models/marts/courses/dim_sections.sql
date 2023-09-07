{# {{
    config(
        materialized='incremental'
    )
}} #}

with sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
    where created_at >= '2023-01-01'
)

select * 
from sections

{# 
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where pulled_at > (select max(event_time) from {{ this }})

{% endif %} #}