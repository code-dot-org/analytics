with 
forms as (
    select * 
    from {{ ref('dim_forms') }}
),

hoc_form_registrations as (
    select 
        form_id
        , form_kind
        , email 
        , hoc_year                                as cal_year
        , school_year
        , registered_at
        , trunc(registered_at)                    as registered_dt
        , event_type
        , email_pref
        , special_event_flag
        , review
        , city
        , state
        , country
    from forms 
    where form_category = 'hoc'
),

hoc_prospects as (
    select *
    from {{ ref('stg_external_datasets__pardot_prospects') }}
),

combined as (
    select 
        form_id as registration_id,
        {# email, #}
        cal_year,
        school_year,
        registered_dt,
        city,
        state,
        country 
    from hoc_form_registrations
    union all 
    select 
        prospect_id,
        {# email, #}
        cal_year::char(4),
        school_year,
        registered_dt,
        city,
        state,
        country 
    from hoc_prospects
),

final as (
    select 
        registration_id,
        cal_year,
        school_year,
        registered_dt,
        city,
        state,
        country
    from combined )

select * -- registration_id, count(*)
from final
-- group by registration_id
-- having count(*)>1