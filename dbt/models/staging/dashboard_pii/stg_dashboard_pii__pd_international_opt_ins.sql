with 
international_opt_ins as (
    select * 
    from {{ ref('base_dashboard_pii__pd_international_opt_ins')}}
),

final as (
    select 
        international_opt_in_id,
        user_id                                                     as teacher_id,
        created_at                                                  as form_submitted_at,
        updated_at                                                  as updated_at,
        json_extract_path_text(form_data, 'firstName')              as first_name,
        json_extract_path_text(form_data, 'firstNamePreferred')     as pref_name,
        json_extract_path_text(form_data, 'lastName')               as last_name,
        json_extract_path_text(form_data, 'email')                  as email,
        json_extract_path_text(form_data, 'emailAlternate')         as email_alt,
        json_extract_path_text(form_data, 'gender')                 as gender,
        json_extract_path_text(form_data, 'schoolDepartment')       as school_department,
        json_extract_path_text(form_data, 'schoolMunicipality')     as school_municipality,
        json_extract_path_text(form_data, 'schoolName')             as school_name,
        json_extract_path_text(form_data, 'schoolCity')             as school_city,
        json_extract_path_text(form_data, 'schoolCountry')          as school_country,
        json_extract_path_text(form_data, 'ages')                   as age_taught,
        json_extract_path_text(form_data, 'subjects')               as subject_taught,
        json_extract_path_text(form_data, 'resources')              as cs_resources,
        json_extract_path_text(form_data, 'robotics')               as robotics_resources,
        json_extract_path_text(form_data, 'date')                   as workshop_date,
        json_extract_path_text(form_data, 'workshopOrganizer')      as workshop_organizer,
        json_extract_path_text(form_data, 'workshopFacilitator')    as workshop_facilitator,
        json_extract_path_text(form_data, 'workshopCourse')         as workshop_course,
        json_extract_path_text(form_data, 'emailOptIn')             as email_opt_in
        {# form_data #}
    from international_opt_ins 
)

select * 
from international_opt_ins 