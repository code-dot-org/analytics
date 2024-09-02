with 

hoc_starts as (
    select * 
    from {{ ref('dim_hoc_starts') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

forms_hoc as (
    select * 
    from {{ ref('dim_hoc_event_registrations') }}
),

-- form_geos as (
--     select * 
--     from {{ ref("stg_pegasus_pii__form_geos") }}
-- ),

users as (
    select * 
    from {{ ref("stg_dashboard__users") }}
),

user_geos as (
    select * 
    from {{ ref("stg_dashboard__user_geos") }}
),

hoc_hits as (
    select 
        date_trunc('month', hoc.started_at)                                                 as hoc_month,
        hoc.school_year,
        hoc.country,
        count(distinct hoc.hoc_start_id)                                                 as total_hoc_hits 
       
    from hoc_starts                                                                      as hoc
    group by 
        hoc_month,
        hoc.school_year,
        hoc.country
),

hoc_event_reg as (
    select 
        date_trunc('month', forms_hoc.registered_at)                                        as registration_month,
        forms_hoc.school_year,
        forms_hoc.country,
        count(distinct forms_hoc.form_id)                                                   as total_event_registrations
    from forms_hoc
    -- left join form_geos 
    --     on forms_hoc.form_id = form_geos.form_id
    group by 
        registration_month,
        forms_hoc.school_year,
        forms_hoc.country
),

new_accounts as (
    select 
        date_trunc('month', users.created_at)                                               as created_month, 
        sy.school_year,
        user_geos.country,
        sum(case when user_type = 'teacher' then 1 end)                                     as total_new_teacher_accounts,
        sum(case when user_type = 'student' then 1 end)                                     as total_new_student_accounts
    from users
    left join user_geos 
        on users.user_id = user_geos.user_id
    left join school_years                                                                  as sy 
        on users.created_at between sy.started_at and sy.ended_at
    where current_sign_in_at is not null
    group by 
        created_month,
        sy.school_year,
        user_geos.country
)

select na.created_month                                                                     as month_of,
       na.school_year,
       na.country,
       na.total_new_teacher_accounts,
       na.total_new_student_accounts,
       reg.total_event_registrations,
       hh.total_hoc_hits
from new_accounts na
left join hoc_event_reg reg 
  on na.created_month = reg.registration_month
  and na.school_year = reg.school_year
  and na.country = reg.country
left join hoc_hits hh
  on hh.hoc_month = reg.registration_month
  and hh.school_year = reg.school_year
  and hh.country = reg.country