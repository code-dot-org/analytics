with 

foorm_simple_survey_forms as (
    select * 
    from {{ ref('stg_dashboard_pii__foorm_simple_survey_forms') }}
),

foorm_simple_survey_submissions as (
    select * 
    from {{ ref('stg_dashboard_pii__foorm_simple_survey_submissions') }}
),

foorm_submissions_reshaped as (
    select * 
    from {{ ref('stg_analysis_pii__foorm_submissions_reshaped') }}
),

foorm_forms_reshaped as (
    select * 
    from {{ ref('stg_analysis__foorm_forms_reshaped') }}
),

foorm_surveys as (
    select
    fssf.path,
    fssf.form_name,
    fsss.simple_survey_form_id,
    fsss.foorm_submission_id,
    fsss.user_id,
    fsss.created_at,
    fsr.item_name,
    fsr.matrix_item_name,
    fsr.response_value,
    fsr.response_text,
    ffr.item_type,
    ffr.item_text,
    ffr.matrix_item_header,
    ffr.response_options,
    ffr.num_response_options

    from foorm_simple_survey_forms                                          as fssf

    join foorm_simple_survey_submissions                                    as fsss 
        on fsss.simple_survey_form_id = fssf.foorm_simple_survey_form_id

    left join foorm_submissions_reshaped                                    as fsr 
        on fsr.submission_id = fsss.foorm_submission_id

    left join foorm_forms_reshaped                                          as ffr 
        on ffr.form_name = fssf.form_name 
        and ffr.form_version = fssf.form_version
        and fsr.item_name = ffr.item_name
),

comments as (
    select distinct 
        foorm_submission_id
        , item_name
        , response_text
        , left(item_name, (position ('-Comment' in item_name)-1))           as item_name_parent
    from foorm_surveys
    where item_name like '%-Comment'
),

questions_multiselect as (
    select  
        f.*
        , replace(replace(split_part(f.response_text, ', ',1),'[',''),']','') sp1 
        , replace(replace(split_part(f.response_text, ', ',2),'[',''),']','') sp2
        , replace(replace(split_part(f.response_text, ', ',3),'[',''),']','') sp3
        , replace(replace(split_part(f.response_text, ', ',4),'[',''),']','') sp4
        , replace(replace(split_part(f.response_text, ', ',5),'[',''),']','') sp5
        , replace(replace(split_part(f.response_text, ', ',6),'[',''),']','') sp6
        , replace(replace(split_part(f.response_text, ', ',7),'[',''),']','') sp7
        , replace(replace(split_part(f.response_text, ', ',8),'[',''),']','') sp8
        , replace(replace(split_part(f.response_text, ', ',9),'[',''),']','') sp9
        , replace(replace(split_part(f.response_text, ', ',10),'[',''),']','') sp10
        , replace(replace(split_part(f.response_text, ', ',11),'[',''),']','') sp11
        , replace(replace(split_part(f.response_text, ', ',12),'[',''),']','') sp12
        , replace(replace(split_part(f.response_text, ', ',13),'[',''),']','') sp13
        , replace(replace(split_part(f.response_text, ', ',14),'[',''),']','') sp14
        , replace(replace(split_part(f.response_text, ', ',15),'[',''),']','') sp15
    from foorm_surveys f
    where item_type = 'multiSelect'
),

multiselect_long as ( 
    select *
    from questions_multiselect
    unpivot (
        multiple_choice for response in (
            sp1
            , sp2
            , sp3
            , sp4
            , sp5
            , sp6
            , sp7
            , sp8
            , sp9
            , sp10
            , sp11
            , sp12
            , sp13
            , sp14
            , sp15
        )
    )
    where len(multiple_choice)>=1
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

users as (
    select * 
    from {{ ref('stg_dashboard_pii__users') }}
)

select distinct 
    s.form_name
    , s.foorm_submission_id
    , s.user_id
    , u.name                                                                        as code_studio_name
    , u.teacher_email                                                               as email
    , trunc(s.created_at)                                                           as created_at
    , s.matrix_item_name
    , s.matrix_item_header
    , s.item_name
    , trim(rtrim(s.item_text,'.'))                                                  as item_text
    , s.item_type 
    , s.response_value
    , case
        when s.item_type = 'multiSelect' then fsl.multiple_choice
        when s.response_value = 'other' then 'other'
        when s.response_value = 'true' then 'Y'
        else trim(s.response_text)
    end                                                                             as response_text
    , case 
        when s.response_value = 'other' 
        then (
            select c.response_text 
            from comments c 
            where s.foorm_submission_id = c.foorm_submission_id 
            and s.item_name = c.item_name_parent
        ) 
        when s.response_value = 'true' then 'Y'
        else trim(s.response_text)
    end                                                                             as full_response_text
    , sy.school_year 
    , coalesce (ss.school_name, si.school_name)                                     as school_name
    , coalesce (ss.state, si.state)                                                 as school_state
    , coalesce (ss.school_type, si.school_type)                                     as school_type
    , si.school_id

from foorm_surveys                                                                  as s

join users                                                                          as u 
    on s.user_id = u.user_id

left join multiselect_long                                                          as fsl 
    on s.foorm_submission_id = fsl.foorm_submission_id 
    and s.item_name = fsl.item_name

left join school_years sy 
    on s.created_at between sy.started_at and sy.ended_at 

left join user_school_infos                                                         as usi  
    on s.user_id = usi.user_id 
    and s.created_at between usi.started_at and coalesce (usi.ended_at, sysdate)

left join school_infos                                                              as si 
    on usi.school_info_id = si.school_info_id

left join schools                                                                   as ss 
    on si.school_id = ss.school_id

where s.item_name not like '%-Comment'