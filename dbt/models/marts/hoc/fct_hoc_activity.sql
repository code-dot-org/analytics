with 
hoc_activity as (
    select * 
    from {{ ref('stg_pegasus_pii__hoc_activity') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

dim_hoc_event_registrations as (
    select * 
    from {{ ref("dim_hoc_event_registrations") }}
),

form_geos as (
    select * 
    from {{ ref("stg_pegasus_pii__form_geos") }}
),

users as (
    select * 
    from {{ ref("stg_dashboard__users") }}
),

user_geos as (
    select * 
    from {{ ref("stg_dashboard__user_geos") }}
),

hoc_hits as (
    select date_trunc('month', hoc.started_at) as hoc_month,
       sy.school_year,
       hoc.country,
       count(distinct hoc.hoc_activity_id) as total_hoc_hits 
       
    from hoc_activity as hoc
    left join school_years as sy on hoc.started_at between sy.started_at and sy.ended_at
    group by hoc_month,
        sy.school_year,
        hoc.country
),

hoc_event_reg as (
    select date_trunc('month', dim_hoc_event_registrations.registered_at) as registration_month,
       sy.school_year,
       form_geos.country,
       count(distinct dim_hoc_event_registrations.form_id) as total_registrations
    from dim_hoc_event_registrations
    left join form_geos on dim_hoc_event_registrations.form_id = form_geos.form_id
    left join school_years as sy on dim_hoc_event_registrations.hoc_year = sy.school_year_int
    group by registration_month,
        sy.school_year,
        form_geos.country
),

new_accounts as (
    select date_trunc('month', users.created_at) as created_month, 
       sy.school_year,
       user_geos.country,
       sum(case when user_type = 'teacher' then 1 end) as total_new_teacher_accounts,
       sum(case when user_type = 'student' then 1 end) as total_new_student_accounts
    from users
    left join user_geos on users.user_id = user_geos.user_id
    left join school_years as sy on users.created_at between sy.started_at and sy.ended_at
    where current_sign_in_at is not null
    group by created_month,
        sy.school_year,
        user_geos.country
)

select na.created_month,
       na.school_year,
       na.country,
       na.total_new_teacher_accounts,
       na.total_new_student_accounts,
       reg.total_registrations,
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