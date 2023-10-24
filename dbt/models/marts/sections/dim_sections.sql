{{
    config(
        materialized='incremental'
    )
}}

with sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
)

select * 
from sections
 
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where created_at > (select max(created_at) from {{ this }})

{% endif %}