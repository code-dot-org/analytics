with 
cs_ambassadors as (
    select *
    from {{ ref('seed_cs_ambassador_app') }}
)

select 
    created_dt
    , teacher_email
    , case when cdo_teacher = 'Yes' then 1 else 0 end as cdo_teacher
    , teacher_first_name   
    , teacher_last_name
    , teacher_email_school
    , teacher_email_cdo
    , teacher_email_alt
    , state
    , courses_taught
    , case 
        when courses_taught like '%CS Principles%'
            or courses_taught like '%AP Computer Science Principles%' 
            or courses_taught like '%AP csp%'
            then 1
        else 0 
    end                                                                         as taught_csp
    , case 
        when 
            courses_taught like '%AP A%' 
            or courses_taught like '%AP Computer Science A%'
            or courses_taught like '%CSA%'
            then 1
        else 0 
    end                                                                         as taught_csa
    , case 
        when 
            courses_taught like '%CS Discoveries%' 
            or courses_taught like '%CSD%'
            then 1
        else 0 
    end                                                                         as taught_csd
    , applicant_type
    , ambassador_first_name
    , ambassador_last_name
    , ambassador_email
    , ambassador_email_receive_comms
    , ambassador_email_alt
    , ambassador_grade
    , parent_email
    , school_name
    , courses_taken
    , case 
        when courses_taken like '%CS Principles%'
            or courses_taken like '%AP Computer Science Principles%' 
            or courses_taken like '%AP csp%'
            then 1
        else 0 
    end                                                                         as took_csp
    , case 
        when 
            courses_taken like '%AP A%' 
            or courses_taken like '%AP Computer Science A%'
            or courses_taken like '%CSA%'
            then 1
        else 0 
    end                                                                         as took_csa
    , case 
        when 
            courses_taught like '%CS Discoveries%' 
            or courses_taught like '%CSD%'
            then 1
        else 0 
    end                                                                         as took_csd
from cs_ambassadors