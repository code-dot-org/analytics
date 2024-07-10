{{
    config(
        materialized='incremental',
        unique_key='date_user'
    )
}}
-- model: int_daily_summary_sign_ins.sql
with summary as (
    select
        user_id,
        sign_in_at::date    as activity_date,
        
        {{ dbt_utils.generate_surrogate_key(
            ['user_id','sign_in_at']) }} as date_user,

        count(*)            as num_sign_ins

    from {{ ref('stg_dashboard__sign_ins') }}

{% if is_incremental() %}

where trunc(sign_in_at) >= (
    select coalesce(
            max(event_time),'1900-01-01'::TIMESTAMP) 
        from {{ this }} )

{% endif %}

{{ dbt_utils.group_by(3) }} 

),

final as (
    select * 
    from summary 
    order by activity_date
)

select * 
from final 