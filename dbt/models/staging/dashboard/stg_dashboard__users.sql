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
        case when user_type = 'student' then user_id end as student_id,
        case when user_type = 'teacher' then user_id end as teacher_id,
<<<<<<< HEAD
        studio_person_id,
        {# primary_contact_info_id,` #}
        user_type,
        locale,
        sign_in_count,
        school_info_id,
        total_lines,  
=======
        user_type,

        studio_person_id,
        school_info_id,
        is_urg,
        gender,
        locale,
        birthday,
        
        sign_in_count,
        total_lines,     
        
>>>>>>> bd4abcd7997aa74af7307fead15856c154c22d4b
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