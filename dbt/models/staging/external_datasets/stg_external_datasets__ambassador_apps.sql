with 
cs_ambassadors as (
    select *
    from {{ ref('seed_cs_ambassador_app') }}
)

select 
    created_dt
    , teacher_email
    , cdo_teacher
    , teacher_first_name   
    , teacher_last_name
    , teacher_email_school
    , teacher_email_cdo
    , teacher_email_alt
    , state
    , replace(
        replace(
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                courses_taught, 'CS Principles', 'csp'), 
                        'AP® CSA', 'csa'), 
                    'CS Discoveries', 'csd'), 
                'AP Computer Science Principles', 'csp'), 
            'AP Computer Science A', 'csa'), 
        'AP A', 'csa'),
    'AP csp', 'csp')
from cs_ambassadors