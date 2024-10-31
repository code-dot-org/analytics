with 
prospects as (
    select *
    from {{ ref('dim_hoc_prospects') }}
),

registrations as (
    select *,
        date_trunc('week',registered_at)        as registered_week,
        date_trunc('month',registered_at)       as registered_month
    from {{ ref('dim_hoc_event_registrations') }}
),

combined as (
    select 
        school_year,
        created_month,
        created_week
        country,
        state,
        count(prospect_id)  as num_prospects,
        null                as num_registrations
    from prospects
    {{ dbt_utils.group_by(5) }} 

    union all 

    select 
        school_year, 
        registered_week,
        registered_month,
        country, 
        state,
        null,
        count(distinct form_id)
    from registrations
    {{ dbt_utils.group_by(5) }} )

select *
from combined