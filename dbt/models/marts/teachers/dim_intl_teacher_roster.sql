with 

pd_intl_opt_ins as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_international_opt_ins') }}
), 

teachers as (
    select * 
    from {{ ref('dim_teachers') }}
)

select 
    oi.teacher_id 
    , oi.form_submitted_at
    , oi.cal_year
    , oi.first_name
    , oi.last_name
    , oi.pref_name 
    , oi.email 
    , oi.email_alt
    , oi.school_department
    , oi.school_municipality
    , oi.school_name
    , oi.school_city 
    , oi.school_country 
    , oi.workshop_date 
    , oi.workshop_organizer 
    , oi.workshop_course_name 
    , oi.email_opt_in
    , t.created_at                                              as account_created_at   
    , t.current_sign_in_at 
    , t.sign_in_count                                            
from pd_intl_opt_ins                                            as oi 
left join teachers                                              as t 
    on oi.teacher_id = t.teacher_id