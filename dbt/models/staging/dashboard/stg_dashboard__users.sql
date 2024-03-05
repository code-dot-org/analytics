{{
    config(
        materialized='incremental',
        unique_key='user_id')
}}

with 
users as (
    select *
    from {{ ref('base_dashboard__users') }}
    where is_active
        and user_type is not null 

    {% if is_incremental() %}

    and updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
),

renamed as (
    select 
        user_id,
        studio_person_id,
        user_type,
        is_urg,
        locale,
        sign_in_count,
        school_info_id,
        total_lines,     
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,  
        deleted_at,   
        purged_at
    from users
)

select * 
from renamed