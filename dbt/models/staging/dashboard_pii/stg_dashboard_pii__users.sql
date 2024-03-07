{{ 
    config(
        materialized='incremental',
        unique_key='user_id')
}}

with
users as (
    select *
    from {{ ref('base_dashboard_pii__users') }}
    where is_active
        and user_type is not null 

    {% if is_incremental() %}

    and updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
),

renamed as (
    select
        user_id,
        user_type,

        case when user_type = 'teacher' then email else null end as teacher_email,

        birthday,
        datediff(year, birthday, current_date) as age_years,
        is_active,
        is_urg,
        races,

        case
            -- If races contains 'hispanic', return 'hispanic'
            when races like '%hispanic%'            then 'hispanic'
            when races like '%,%' and is_urg = 1    then 'two or more'
            when races is not null then null 
        else null end as race_group,

        nullif(lower(gender), '') as gender,
        
        case when lower(gender) in ('m','f','n','o') then lower(gender)
            else null end as gender_group,

        created_at,
        updated_at,
        purged_at,
        deleted_at
    from users
)

select *
from renamed