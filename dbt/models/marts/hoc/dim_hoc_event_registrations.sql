with 
forms as (
    select * 
    from {{ ref('dim_forms') }}
)

--2024 used Pardot rather than Pegasus, so these need to be added separately
, registrations2024 as (
    select * 
    from {{ref('stg_analysis_pii__hoc_event_registrations2024')}}
)

, teachers as (
    select * 
    from {{ref('dim_teachers')}}
)

, schools as (
    select *
    from {{ref('dim_schools')}}
)

, teacher_school_historical as (
    select * 
    from {{ref('int_teacher_schools_historical')}}
    where school_id is not null
)

, districts as (
    select * from {{ref('dim_districts')}}
)

, pegasus_registrations as (
    select 
        form_id
        --, form_kind
        , email
        , hoc_year                                as cal_year
        , school_year
        , registered_at
        , event_type
        , forms.city
        , forms.state
        , forms.country
        , null as language
    from forms 
    where form_category = 'hoc'
)

, pardot_registrations as (
    select
        null as form_id
        , email as email
        , '2024' as cal_year
        , '2024-25' as school_year
        , last_submitted as registered_at
        , null as event_type
        , null as city
        , null as state
        , country
        , language
    from {{ref('stg_analysis_pii__hoc_event_registrations2024')}}
)

--select * from pegasus_registrations

, combined as (
    select * from pardot_registrations
    union all
    select * from pegasus_registrations
)

select * from combined


/*

        , {{get_email_domain('email')}}
        , teachers.teacher_id
        , tsh.school_id
        --, schools.school_district_id

    from forms 
    left join teachers
        on forms.email = teachers.teacher_email
    left join teacher_school_historical tsh
        on teachers.teacher_id = tsh.teacher_id
        and registered_at between tsh.started_at and tsh.ended_at
    left join schools 
        on tsh.school_id = schools.school_id
)

select
    pegasus_registrations.*,
    districts.school_district_id as district_imputed
from pegasus_registrations
 left join districts
        on pegasus_registrations.email_domain = districts.domain_name

*/