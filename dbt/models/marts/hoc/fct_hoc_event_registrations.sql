with 
prospects as (
    select *
    from {{ ref('dim_hoc_prospects') }}
),

registrations as (
    select *,
        date_trunc('week',registered_at)        as registered_week,
        date_trunc('month',registered_at)       as registered_month,
        date_trunc('year',registered_at)        as registered_year
    from {{ ref('dim_hoc_event_registrations') }}
),

combined as (
    select 
        school_year, 
        registered_year,
        registered_week,
        registered_month,
        country, 
        state,
        city,
        count(distinct form_id) as num_registrations
    from registrations
    {{ dbt_utils.group_by(7) }}

    union all 
    select 
        school_year,
        created_year,
        created_month,
        created_week,
        country,
        state,
        city,
        count(prospect_id)  as num_registrations
    from prospects
    {{ dbt_utils.group_by(7) }} 
),

final as (
    select 
        school_year, 
        registered_year,
        registered_week, 
        registered_month,
        country,
        state,
        city,
        num_registrations
    from combined
    order by school_year desc ) 

select * 
from final