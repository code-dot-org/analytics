{{
    config(
        materialized='incremental'
    )
}}

with sections as (
    select * 

    {% if is_incremental() %}

    from {{ ref('stg_dashboard__sections') }}
    
    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this model)
    where created_at > (select max(created_at) from {{ this }})

    {% endif %}
)

select * 
from sections
 