with 
forms as (
    select * 
    from {{ ref('stg_pegasus_pii__forms') }}
),

form_geos as (
    select * 
    from {{ ref('stg_pegasus_pii__form_geos') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        forms.form_id,
        forms.form_category,
        forms.hoc_year,
        sy.school_year, 
        forms.email,
        forms.name,
        forms.form_kind,
        forms.updated_at,
        forms.indexed_at,
        forms.review,
        forms.reviewed_by,
        forms.reviewed_at,
        forms.processed_at,
        forms.notified_at,
        forms.user_id,
        forms.parent_id,
        forms.location_country_code,
        forms.event_type,
        forms.email_pref,
        forms.special_event_flag,
        coalesce(forms.city,form_geos.city)                     as city,
        coalesce(forms.state,form_geos.state)                   as state,
        coalesce(forms.country,form_geos.country)               as country,
        max(forms.created_at)                                   as registered_at,
        max(coalesce(forms.updated_at,forms.created_at))        as last_updated_at
    from forms 
    left join form_geos 
        on forms.form_id = form_geos.form_id
    left join school_years                                      as sy
        on forms.hoc_year = sy.school_year_int
    {{ dbt_utils.group_by(23) }}
)

select * 
from combined