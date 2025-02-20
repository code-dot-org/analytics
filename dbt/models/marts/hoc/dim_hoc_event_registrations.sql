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

, combined as (
    select * from pardot_registrations
    union all
    select * from pegasus_registrations
)

, with_supplementary as (
    select
        combined.* 
        , {{get_email_domain('email')}}
        , teachers.teacher_id
        , tsh.school_id as school_id
        , RANK() OVER (
            PARTITION BY combined.email, combined.form_id
            ORDER BY teachers.teacher_id DESC --this accounts for a single email matching to multiple accounts. It only pulls the most recent. 
    ) AS teacher_account_rank
    from combined
    left join teachers 
        on combined.email = teachers.teacher_email
    left join teacher_school_historical tsh
        on teachers.teacher_id = tsh.teacher_id
        and registered_at between tsh.started_at and tsh.ended_at
)

, final as (
    select
        form_id
        , cal_year
        , school_year
        , registered_at
        , event_type
        , with_supplementary.city
        , with_supplementary.state
        , with_supplementary.country
        , with_supplementary.language
        , with_supplementary.teacher_id
        , schools.school_id
        , coalesce(schools.school_district_id, districts.school_district_id) as school_district_id --districts is based on email domain not on school_id
    from 
        with_supplementary
    left join schools 
        on with_supplementary.school_id = schools.school_id
    left join districts
        on with_supplementary.email_domain = districts.domain_name
    where teacher_account_rank = 1
)

select * from final
