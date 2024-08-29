with 
international_opt_ins as (
    select * 
    from {{ ref('base_dashboard_pii__pd_international_opt_ins')}}
),

final as (
    select 
        international_opt_in_id,
        user_id                                                                                         as teacher_id,
        created_at                                                                                      as form_submitted_at,
        extract(year from created_at)                                                                   as cal_year,
        updated_at                                                                                      as updated_at,
        lower(json_extract_path_text(form_data, 'firstName'))                                           as first_name,
        lower(json_extract_path_text(form_data, 'firstNamePreferred'))                                  as pref_name,
        lower(json_extract_path_text(form_data, 'lastName'))                                            as last_name,
        json_extract_path_text(form_data, 'email')                                                      as email,
        json_extract_path_text(form_data, 'emailAlternate')                                             as email_alt,
        lower(json_extract_path_text(form_data, 'gender'))                                              as gender,
        lower(json_extract_path_text(form_data, 'schoolDepartment'))                                    as school_department,
        lower(json_extract_path_text(form_data, 'schoolMunicipality'))                                  as school_municipality,
        lower(json_extract_path_text(form_data, 'schoolName'))                                          as school_name,
        lower(json_extract_path_text(form_data, 'schoolCity'))                                          as school_city,
        lower(json_extract_path_text(form_data, 'schoolCountry'))                                       as school_country,
        json_extract_path_text(form_data, 'ages')                                                       as age_taught,
        json_extract_path_text(form_data, 'subjects')                                                   as subject_taught,
        json_extract_path_text(form_data, 'resources')                                                  as cs_resources,
        json_extract_path_text(form_data, 'robotics')                                                   as robotics_resources,
        json_extract_path_text(form_data, 'date')                                                       as workshop_date,
        lower(json_extract_path_text(form_data, 'workshopOrganizer'))                                   as workshop_organizer,
        lower(json_extract_path_text(form_data, 'workshopFacilitator'))                                 as workshop_facilitator,
        case 
            when 
                lower(json_extract_path_text(form_data, 'workshopCourse')) in (
                    'cs discoveries'
                    , 'csd'
                    , 'descubrimientos de ciencias de la computación')
                then 'csd'
            when 
                lower(json_extract_path_text(form_data, 'workshopCourse')) in (
                    'cs fundamentals (courses a-f)'
                    , 'csf_af'
                    , 'fundamentos de ciencias de la computación (cursos a-f)'
                    , 'fundamentos de ciencias de la computación. (curso a-f)'
                    , 'cs fundamentals (pre-express or express)'
                    , 'csf_express'
                    , 'fundamentos de ciencias de la computacion (express o pre-express)'
                    , 'fundamentos de ciencias de la computación (pre-express o express)'
                    , 'fundamentos de cs (pre-express o express)')
                then 'csf'
            when 
                lower(json_extract_path_text(form_data, 'workshopCourse')) in (
                    'cs principles'
                    , 'csp'
                    , 'principios de ciencias de la computación')
                then 'csp'
            when 
                lower(json_extract_path_text(form_data, 'workshopCourse')) in (
                    'csa')
                then 'csa'
            when 
                lower(json_extract_path_text(form_data, 'workshopCourse')) in (
                    'not applicable'
                    , 'other')
                then 'other'
            else lower(json_extract_path_text(form_data, 'workshopCourse'))
        end                                                                                         as workshop_course_name,
        case 
            when lower(json_extract_path_text(form_data, 'emailOptIn')) in (
                'opt_in_yes'
                , 'sí'
                , 'yes'
                , 'כן')
                then 1 
            else 0 
        end                                                                                         as email_opt_in
        {# form_data #}
    from international_opt_ins 
)

select * 
from final 