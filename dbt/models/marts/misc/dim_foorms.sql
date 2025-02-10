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
)
,

foorm_surveys as (
    select
    fssf.path,
    fssf.form_name,
    fsss.simple_survey_form_id,
    fssf.kind,
    fssf.form_version,
    fsss.foorm_submission_id,
    fsss.user_id,
    fsss.created_at,
    case -- adjustment for surveys/teachers/young_women_in_cs
         when fsr.item_name = 'pre_enrollment' then 'num_pre_enrollment'
         when fsr.item_name = 'post_enrollment' then 'num_post_enrollment'
         else fsr.item_name
     end as item_name,
    fsr.matrix_item_name,
    fsr.response_value,
    case 
      when fsr.response_value SIMILAR TO '[0-9]+(.[0-9][0-9])?' THEN response_value::integer
      WHEN ffr.item_type = 'scale' AND fsr.response_text SIMILAR TO '[0-9]+(.[0-9][0-9])?' THEN response_text::integer -- type 'scale' doesn't have a response value, only text. 
    ELSE NULL
    END response_value_numeric,
    coalesce( nullif(fsr.response_text,''), fsr.response_value) as response_text,
    ffr.item_type,
    ffr.item_text,
    ffr.matrix_item_header,
    ffr.response_options,
    ffr.num_response_options,
    ffr.is_facilitator_specific,
    JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(fssf.properties,'survey_data'),'course') course

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
    from {{ ref('dim_users') }}
)

select distinct 
    s.form_name
    , s.path
    , s.kind
    , s.simple_survey_form_id
    , s.form_version
    , s.course
    , s.is_facilitator_specific
    , s.foorm_submission_id                                                        as submission_id
    , s.user_id
    , u.user_type
    , u.us_intl
    , u.country
    , trunc(s.created_at)                                                           as submission_date
    , s.matrix_item_name
    , s.matrix_item_header
    , s.item_name
    , trim(rtrim(s.item_text,'.'))                                                  as item_text
    , s.item_type 
    , s.num_response_options
        , case
        when s.item_type = 'multiSelect' then fsl.multiple_choice
        when s.response_value = 'other' then 'other'
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
        else trim(s.response_text)
    end                                                                             as full_response_text
    , s.response_value
    , s.response_value_numeric
    , s.response_value_numeric ::float/s.num_response_options::float                as response_value_pct
    , s.response_options
    , sy.school_year 
    , si.school_id
    , coalesce (ss.school_name, si.school_name)                                     as school_name
    , coalesce (ss.state, si.state)                                                 as school_state
    , coalesce (ss.school_type, si.school_type)                                     as school_type

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