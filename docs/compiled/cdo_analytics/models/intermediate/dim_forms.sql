with 
forms as (
    select * 
    from "dev"."dbt_allison"."stg_pegasus_pii__forms"
),

form_geos as (
    select * 
    from "dev"."dbt_allison"."stg_pegasus_pii__form_geos"
),

combined as (
    select 
        forms.form_id,
        forms.form_category,
        forms.hoc_year,
        forms.email,
        forms.name,
        forms.form_kind,
        forms.form_data,
        forms.updated_at,
        forms.indexed_at,
        forms.review,
        forms.reviewed_by,
        forms.reviewed_at,
        forms.processed_at,
        forms.processed_data, 
        forms.notified_at,
        forms.user_id,
        forms.parent_id,
        forms.location_country_code,
        forms.form_data_text,  
        forms.processed_data_text,
        coalesce(forms.city,form_geos.city)                     as city,
        coalesce(forms.state,form_geos.state)                   as state,
        coalesce(forms.country,form_geos.country)               as country,
        max(forms.created_at)                                   as registered_at,
        max(coalesce(forms.updated_at,forms.created_at))        as last_updated_at
    from forms 
    left join form_geos 
        on forms.form_id = form_geos.form_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)

select * 
from combined