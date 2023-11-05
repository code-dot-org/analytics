with
pd_enrollments as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_enrollments') }}
),

pd_attendances as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_attendances') }}
),

pd_applications as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_applications') }}
),

pd_sessions as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_sessions') }}
),

pd_workshops as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_workshops') }}
),

sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
),

followers as (
    select * 
    from {{ ref('stg_dashboard__followers') }}
),

forms as (
    select * 
    from {{ ref('stg_pegasus_pii__forms') }}
),

pd_workshop_based as (
    select 
        pde.user_id, 
        pdw.pd_workshop_id                                                                      as workshop_id,
        pdw.section_id                                                                          as section_id,  
        case 
            when lower(pdw.subject) in ('intro workshop', 'intro') or pdw.subject is null
            then 'Intro Workshop'
            else pdw.subject end                                                                as subject,
        min(pds.started_at)::date                                                               as trained_at -- actual workshop date

    from pd_enrollments pde
    join pd_attendances pda on pda.pd_enrollment_id = pde.pd_enrollment_id
    join pd_workshops pdw on pdw.pd_workshop_id = pde.pd_workshop_id
    join pd_sessions pds on pds.pd_workshop_id = pdw.pd_workshop_id
    where course = 'CS Fundamentals'
    and (lower(pdw.subject) in ('intro workshop', 'intro', 'deep dive', 'district') or pdw.subject is null)
    and pda.deleted_at is null
    and pde.user_id is not null
    and pds.deleted_at is null 
    {{ dbt_utils.group_by(4) }}
), 

  -- second: find any teachers who attended a section used for CSF PD in the old data model
pegasus_form_based as (
    select 
        f.student_user_id                                                   as user_id,
        null::int                                                           as workshop_id,
        se.section_id,
        'Intro Workshop'                                                    as subject,
        case 
            when len(form_data_text) < 65535 
            then 
            to_date(
                nullif(
                    case json_extract_path_text( json_extract_array_element_text( json_extract_path_text(form_data_text, 'dates'), 0), 'date_s')
                        when '08/10/2015' then to_char(to_date('08/10/2015', 'MM/DD/YYYY'), 'MM/DD/YY') -- fix malformed data
                        when '7/19/2016' then to_char(to_date('07/19/2016', 'MM/DD/YYYY'), 'MM/DD/YY') -- fix malformed data
                        else json_extract_path_text( json_extract_array_element_text( json_extract_path_text(form_data_text, 'dates'), 0), 'date_s')
                        end,
                    ''),
                'MM/DD/YY'
            )
            else null
        end                                                                 as trained_at
    from forms
    join sections se on se.section_id = nullif(json_extract_path_text(form_data_text, 'section_id_s'),'')::int
    join followers f on f.section_id = se.section_id
    where lower(forms.form_kind) = 'professionaldevelopmentworkshop'
    and nullif(json_extract_path_text(forms.form_data_text, 'section_id_s'),'') is not null
),

  -- third: add in any teachers in sections that aren't in the forms table, but are labeled "csf_workshop"
section_based as (
    select distinct  
        f.student_user_id                                                  as user_id, 
        null::int                                                          as workshop_id,
        se.section_id,
        'Intro Workshop'                                                   as subject,
        date_trunc('day', se.created_at)::date                             as trained_at

    from followers f
    join sections se ON se.section_id = f.section_id
    where se.section_type = 'csf_workshop'
    and se.section_id not in (select section_id from pegasus_form_based where section_id is not null)
    and se.section_id not in (select section_id from pd_workshop_based where section_id is not null)
)

select * from pd_workshop_based

union all

select * from pegasus_form_based

union all

select * from section_based