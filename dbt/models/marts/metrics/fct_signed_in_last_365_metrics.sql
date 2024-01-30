{{ 
config(
  materialized = 'incremental',
  unique_key = 'sign_in_date'
) 
}}

with sign_ins as (
    select *
    from {{ ref('base_dashboard__sign_ins') }} --change stg_signins after merge
),

all_dates as ( -- replace this with all actual dates from calendar
    select distinct sign_in_at::date as sign_in_date
    from sign_ins
    where sign_in_date > '2024-01-27' -- remove once built
),

users as (
    select *
    from {{ ref('stg_dashboard__users') }}
),

user_geos as (
    select *
    from {{ ref('stg_dashboard__user_geos') }}
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

activity_counts as (
    select
        ad.sign_in_date,
        u.user_type,
        ug.country,
        ug.is_international,
        sy.school_year,
        count(distinct si.user_id) as num_users

    from all_dates as ad
    inner join
        sign_ins as si
        on
            si.sign_in_at between dateadd(
                year, -1, ad.sign_in_date
            )::date and dateadd(day, -1, ad.sign_in_date)::date
    inner join users as u on si.user_id = u.user_id
    left join user_geos as ug on si.user_id = ug.user_id
    left join
        school_years as sy
        on ad.sign_in_date between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(5) }}
)

select * from activity_counts


{% if is_incremental() %}

    -- This filters the data to only what's new or changed
    where sign_in_date > (select max(sign_in_date) from {{ this }})

{% endif %}
