{{
    config(
        materialized='incremental',
        unique_key='user_id')
}}

with
users as (
    select *
    from {{ ref('base_dashboard_pii__users') }}
    where
        is_active

        {% if is_incremental() %}

            and updated_at > (select max(updated_at) from {{ this }})

        {% endif %}
),

renamed as (
    select
        -- PK
        user_id,

        -- FK's
        studio_person_id,
        email,
        user_type,

        --PII
        races,

        -- user info
        is_urg,
        locale,
        sign_in_count,

        school_info_id,

        total_lines,
        current_sign_in_at,

        last_sign_in_at,

        -- misc.
        created_at,
        updated_at,
        deleted_at,
        purged_at,

        -- dates         
        case when user_type = 'student' then user_id end as student_id,
        case when user_type = 'teacher' then user_id end as teacher_id,
        datediff(year, birthday, current_date) as age_years,
        nullif(lower(gender), '') as gender,
        case
            when gender = 'm' then 'm'
            when gender = 'f' then 'f'
            when gender = 'n' then 'nb' ------  non-binary
            when gender = 'o' then 'nl' ------  not listed
            ----  empty string or '-'
            when gender = '' or gender = '-' then 'no_response'
            when gender is null then 'not_collected'
            else 'unexpected value: ' || gender
        end as gender_group,
        case
            -- If races contains 'hispanic', return 'hispanic'
            when races like '%hispanic%' then 'hispanic'

            -- If races matches any of these specific strings, return 'no_response'
            when
                races in ('closed_dialog', 'nonsense', 'opt_out')
                then 'no_response'

            -- If races contains a comma AND is_urg return 'two or more urg'
            when races like '%,%' and is_urg = 1 then 'two_or_more_urg'

            -- If races contains a comma and not caught by case above, then urg is 0 or null, return 'two or more non urg'
            when races like '%,%' then 'two_or_more_non_urg'

            -- If races is NULL, return NULL
            when races is null then 'not_collected'

            -- Default case: return the input value
            else races -- Additional logic may be required here
        end as race_group
    from users
)

select *
from renamed
