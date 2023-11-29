{{
    config(
        materialized='incremental',
        unique_key='user_level_id'
    )
}}

with 
user_levels as (
    select

        max(user_level_id) user_level_id,
        user_id,
        level_id,
        script_id,
        level_source_id,
        max(attempts) attempts,
        created_at,
        updated_at,
        best_result,
        max(time_spent) time_spent,
        is_submitted,
        is_read_only_answers,
        unlocked_at

    from {{ ref('base_dashboard__user_levels') }}


    {% if is_incremental() %}

    where coalesce(created_at,updated_at) > (select max(coalesce(created_at,updated_at)) from {{ this }} )
    group by 
        user_id,
        level_id,
        script_id,
        level_source_id,
        created_at,
        updated_at,
        best_result,
        is_submitted,
        is_read_only_answers,
        unlocked_at
    
    {% endif %}
)

select * 
from user_levels