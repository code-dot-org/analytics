with 
csf_teachers_trained as (
    select *
    from {{ ref('dim_csf_teachers_trained') }}
),

pd_workshops as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_workshops') }}
),

pd_sessions as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_sessions') }}
),

pd_enrollments as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_enrollments') }}
),

pd_attendances as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_attendances') }}
),

pd_workshops_facilitators as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_workshop_facilitators') }}
),

forms as (
    select * 
    from {{ ref('stg_pegasus_pii__forms') }}
),

sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
),

users as (
    select * 
    from {{ ref('stg_dashboard_pii__users') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
),

school_stats as (
    select * 
    from {{ ref('fct_school_stats') }}
),

user_geos as (
    select * 
    from {{ ref('stg_dashboard__user_geos') }}
),

training_school_years as (
    select * 
    from {{ ref('seed_training_school_years') }}
),

state_abbreviations as (
    select * 
    from {{ ref('seed_state_abbreviations') }}
),

regional_partners as (
    select * 
    from {{ ref('stg_dashboard_pii__regional_partners') }}
),

pd_regional_partner_mappings as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_regional_partner_mappings') }}
),

tt_workshop_and_section_ids as(
    select distinct
        workshop_id, 
        section_id 
    from csf_teachers_trained
  ),
  
manual as ( -- manually entered states and zips for workshops that did not match previously 
    select 
        workshop_id, 
        'manual matching'                           as processed_location, 
        state_workshop                              as state,
        zip_workshop::varchar                       as zip, 
        null::int                                   as section_id
    from {{ ref('seed_workshop_state_zip_manual') }}
  ),

zip_processed as ( -- extract state and zip from pd_workshops with a properly formatted address
    -- get state for entries where formatted_address looks like
    -- "formatted_address":"1516 N 35th Ave, Phoenix, AZ 85009, USA"
    -- better than taking first two character capitol letters to avoid "SE 53rd St"
    select 
        workshop_id,
        processed_location,
        substring(SPLIT_PART(JSON_EXTRACT_PATH_TEXT(processed_location,'formatted_address'),',',3),2,2)     as state,
        substring(SPLIT_PART(JSON_EXTRACT_PATH_TEXT(processed_location,'formatted_address'),',',3),5,5)     as zip, 
        pdw.section_id
    from pd_workshops pdw
    join tt_workshop_and_section_ids tt 
    on pdw.pd_workshop_id = tt.workshop_id
    where regexp_instr(SPLIT_PART(JSON_EXTRACT_PATH_TEXT(processed_location,'formatted_address'),',',3),'[0-9][0-9][0-9][0-9][0-9]') != 0
    and pdw.pd_workshop_id not in (select workshop_id from manual where workshop_id is not null)
  ),

other_processed as ( -- extract state and zip from pd_workshops with addresses formatted other ways 
    -- get state for entries where processed_location looks like
    -- {"latitude":47.6062095,"longitude":-122.3320708,"city":"Seattle","state":"Washington","formatted_address":"Seattle, WA, USA"}
    -- or
    -- {"latitude":30.267153,"longitude":-97.7430608,"formatted_address":"Austin, TX, USA"}
    select 
        pdw.pd_workshop_id,
        processed_location,
        case
            when pd_workshop_id in (3604, 876, 2976, 1952, 1951, 2881, 2989, 1949) then 'FL'
            when pd_workshop_id = 2281	then 'MS'
            when pd_workshop_id = 2920	then 'IL'
            when pd_workshop_id = 2914	then 'IL'
            when pd_workshop_id = 2233	then 'MO'
            when pd_workshop_id = 2356	then 'KS'
            when pd_workshop_id = 3966	then 'LA'
            when pd_workshop_id = 2460	then 'TX'
            when pd_workshop_id = 1928	then 'ID'
            when pd_workshop_id = 2124	then 'AZ'
            when pd_workshop_id = 2477	then 'CA'
            when pd_workshop_id = 2649	then 'OR'
            when pd_workshop_id = 5132	then 'WA'
            when pd_workshop_id in (5261, 5262, 5262) then 'IA'
            when pd_workshop_id = 5060 then 'ID'
            when pd_workshop_id = 4865 then 'WV'
            when json_extract_path_text(processed_location, 'state') != '' then json_extract_path_text(processed_location, 'state')
        else regexp_substr(
            json_extract_path_text(processed_location, 'formatted_address')
            , '[A-Z][A-Z]')
        end as state,
        case
            when pd_workshop_id in (876, 2976, 1952, 1951, 2881, 2989, 1949) then '33172'
            when pd_workshop_id = 3604 then '32801'
            when pd_workshop_id = 2281	then '38801'
            when pd_workshop_id = 2920	then '60134'
            when pd_workshop_id = 2914	then '62501'
            when pd_workshop_id = 2233	then '64106'
            when pd_workshop_id = 2356	then '67801'
            when pd_workshop_id = 3966	then '71115'
            when pd_workshop_id = 2460	then '75203'
            when pd_workshop_id = 1928	then '83333'
            when pd_workshop_id = 2124	then '85296'
            when pd_workshop_id = 2477	then '92782'
            when pd_workshop_id = 2649	then '97366'
            when pd_workshop_id = 5132	then '98087'
            when pd_workshop_id in (5261, 5262, 5262) then '52401'
            when pd_workshop_id = 5060 then '83647'
            when pd_workshop_id = 4865 then '26506'
        else regexp_substr(
            json_extract_path_text(processed_location, 'formatted_address')
            , '[0-9][0-9][0-9][0-9][0-9]', -15) 
        end as zip, 
        pdw.section_id
    from pd_workshops pdw
    join tt_workshop_and_section_ids tt 
    on pdw.pd_workshop_id = tt.workshop_id
    where course = 'CS Fundamentals'
    and pdw.pd_workshop_id not in (select workshop_id from zip_processed where workshop_id is not null)
    and pdw.pd_workshop_id not in (select workshop_id from manual where workshop_id is not null)
   ),

workshop_state_zip as (

    select *
    from manual

    union all

    select *
    from zip_processed

    union all

    select *
    from other_processed
 ),

sections_locations as ( --pulls location data out of pegasus_forms workshops
    select distinct
        nullif(json_extract_path_text(form_data_text, 'section_id_s'),'')::int as workshop_id, -- using the section id as the workshop id
        processed_data_text as processed_location,
        json_extract_path_text(processed_data_text, 'location_state_s') as state,  
        json_extract_path_text(processed_data_text, 'location_postal_code_s') as zip,
        null::int as section_id
    from forms 
    join tt_workshop_and_section_ids tt 
    on forms.form_kind = 'ProfessionalDevelopmentWorkshop'
    and nullif(json_extract_path_text(form_data_text, 'section_id_s'),'') =  tt.section_id   
    where zip is not null  
    and zip != ''
    and nullif(json_extract_path_text(form_data_text, 'section_id_s'),'')::int not in (select workshop_id from manual where workshop_id is not null)
   ),

sections_schools as ( --gets location based on the school of the facilitator for workshops in pegasus that did not have a zip location, as well as other sections
    select 
        se.section_id as workshop_id, 
        'based on facilitator school' as processed_location,
        ss_user.state as state, 
        ss_user.zip as zip,      
        null::int as section_id
    from sections se 
    join tt_workshop_and_section_ids tt 
    on se.section_id =  tt.section_id 
    join users u  -- users just needed to get school_info_id
    on se.user_id = u.user_id
    join school_infos si_user
    on si_user.school_info_id = u.school_info_id
    join school_stats ss_user
    on ss_user.school_id = si_user.school_id
    where ss_user.zip is not null 
    and ss_user.zip != ''
    and se.section_id not in (select workshop_id from sections_locations where workshop_id is not null)
    and se.section_id not in (select workshop_id from manual where workshop_id is not null)          
),

sections_geos as ( -- matches any leftover workshops from above based on faciliator's user_geo
    select
        se.section_id as workshop_id,
        'based on facilitator geo' as processed_location, 
        case 
            when se.user_id = 1423830 then 'OH' 
            else ug.state 
        end as state, -- 1423830 is only facilitator in this list not with user_geos in the US 
        case 
            when se.user_id = 1423830 
            then '44113' 
            else ug.postal_code 
        end as zip,
        null::int as section_id
    from sections se 
    join tt_workshop_and_section_ids tt 
    on se.section_id =  tt.section_id 
    join user_geos ug
    on se.user_id = ug.user_id
    where se.section_id not in (select workshop_id from sections_schools where workshop_id is not null)
      and se.section_id not in (select workshop_id from sections_locations where workshop_id is not null)
      and se.section_id not in (select workshop_id from manual where workshop_id is not null) 
),

section_state_zip as (
    select *
    from manual

    union all 

    select *
    from sections_locations

    union all 

    select *
    from sections_schools

    union all

    select *
    from sections_geos
 ),
 
pd_workshop_based as ( 
    select 
        pde.user_id                                                         as user_id,
        pdw.course                                                          as course,
        pdw.pd_workshop_id                                                  as workshop_id,
        pdw.section_id                                                      as section_id,
        case
            when (pdw.subject in ('Intro Workshop', 'Intro') or pdw.subject is null) then 'Intro Workshop' 
            else pdw.subject 
        end                                                                 as subject,
        min(pds.started_at)::date                                           as workshop_date,
        pdw.ended_at                                                        as workshop_date_pdw_ended_at, 
        date_part(month, workshop_date)                                     as month_workshop,
        date_part(dayofweek, workshop_date)                                 as day_of_week_workshop,
        case 
            when pdw.is_on_map = 1 then 'Public' 
            when pdw.is_on_map = 0 then 'Private' 
            else null 
        end                                                                 as audience,
        pdw.is_funded                                                       as funded,
        pdw.funding_type,
        capacity,
        case 
            when pdw.regional_partner_id is not null then 1 
            else 0 
            end                                                             as trained_by_regional_partner,-- using this definition for now for regional_partner_dash
        case   
            when pdw.funding_type = 'partner' then 1 
            else 0 
            end                                                             as trained_by_regional_partner_truth,  -- temporary until we figure out how ed team wants to present data to RPs
        case 
            when rp1.regional_partner_name is not null then rp1.regional_partner_name
            when rp2.regional_partner_name is not null then rp2.regional_partner_name
            else 'No Partner' 
            end                                                             as regional_partner_name,
        coalesce (pdw.regional_partner_id, rpm.regional_partner_id)         as regional_partner_id,
        wsz.zip                                                             as zip,
        coalesce(sa.state_abbreviation, wsz.state)                          as state,
        wsz.processed_location                                              as processed_location,
        u.name                                                              as facilitator_name,
        u.studio_person_id                                                  as studio_person_id_facilitator,
        sy.school_year,
        min(
            case
                when pds.started_at < dateadd(day,-3,getdate()) and pda.pd_attendance_id is null then 1 
                else 0 
                end)                                                        as not_attended,
        case
            when pds.started_at > getdate() then 1 
            else 0 
            end                                                             as future_event
    from pd_workshops pdw   -- does not start with teachers_trained_because this table also keeps track of workshops planned for the future
    join pd_sessions pds 
    on pdw.pd_workshop_id = pds.pd_workshop_id  
    left join pd_enrollments pde
    on pdw.pd_workshop_id = pde.pd_workshop_id
    left join pd_attendances pda 
    on pde.pd_enrollment_id = pda.pd_enrollment_id
    and pda.deleted_at is null 
    left join pd_workshops_facilitators pdf
    on pdw.pd_workshop_id = pdf.pd_workshop_id
    left join users u
    on u.user_id = pdf.user_id
    left join training_school_years sy 
    on pds.started_at between sy.started_at and sy.ended_at
    left join workshop_state_zip wsz 
    on wsz.workshop_id = pdw.pd_workshop_id
    left join state_abbreviations sa
    on sa.state_name = wsz.state 
        or sa.state_abbreviation = wsz.state
    left join regional_partners rp1
    on pdw.regional_partner_id = rp1.regional_partner_id   
    left join pd_regional_partner_mappings rpm 
    on rpm.state = sa.state_abbreviation 
        or rpm.zip_code = wsz.zip
    left join regional_partners rp2 
    on rpm.regional_partner_id = rp2.regional_partner_id  
    where pdw.course = 'CS Fundamentals'
    and (pdw.subject in ( 'Intro Workshop', 'Intro', 'Deep Dive', 'District')  or pdw.subject is null)
    and pds.deleted_at is null
    group by 1, 2, 3, 4, 5, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, u.name, 22, 23, 25
  ),
  
sections_based as ( 
    select distinct 
        tt.user_id                                                              as user_id,
        'CS Fundamentals'                                                       as course,
        coalesce(tt.workshop_id, tt.section_id)                                 as workshop_id,
        tt.section_id                                                           as section_id, -- using section_id as workshop_ids for those workshops not in pd_workshops
        'Intro Workshop'                                                        as subject,
        tt.trained_at                                                           as workshop_date,
        tt.trained_at                                                           as workshop_date_pdw_ended_at,
        date_part(month, tt.trained_at)                                         as month_workshop,
        date_part(dayofweek, tt.trained_at)                                     as day_of_week_workshop,
        JSON_EXTRACT_PATH_TEXT(forms.form_data_text,'type_s')                   as audience,
        null::smallint                                                          as funded,
        null                                                                    as funding_type,
        case 
            when trim(JSON_EXTRACT_PATH_TEXT(forms.form_data_text,'capacity_s')) ~ '^[0-9]+$' 
            then trim(JSON_EXTRACT_PATH_TEXT(forms.form_data_text,'capacity_s'))  
            else null  
        end::int                                                                as capacity,
        0 as trained_by_regional_partner,
        0 as trained_by_reginal_partner_truth,  -- temporary until we figure out how ed team wants to present data to RPs
        case 
            when rp.regional_partner_name is not null then rp.regional_partner_name 
            else 'No Partner' 
            end                                                                 as regional_partner_name,
        rpm.regional_partner_id                                                 as regional_partner_id,
        ssz.zip                                                                 as zip,
        coalesce(sa.state_abbreviation, ssz.state)                              as state,
        ssz.processed_location,
        coalesce(u.name, forms.name)                                            as facilitator_name,
        u.studio_person_id                                                      as studio_person_id_facilitator,
        sy.school_year, 
        0                                                                       as not_attended,
        0                                                                       as future_event
    from csf_teachers_trained tt
    join sections se 
    on se.section_id = tt.section_id 
    join users u -- join to get facilitator data
    on u.user_id = se.user_id
    join training_school_years sy 
    on tt.trained_at between sy.started_at and sy.ended_at
    left join section_state_zip ssz 
    on ssz.workshop_id = tt.section_id 
    left join state_abbreviations sa
    on sa.state_name = ssz.state 
        or sa.state_abbreviation = ssz.state -- join the sa table wether or not the ssz column is long form of name or short
    left join forms -- join to get additional data on the workshop
    on forms.form_kind = 'ProfessionalDevelopmentWorkshop'
    and tt.section_id = nullif(json_extract_path_text(form_data_text, 'section_id_s'),'')::int 
    left join pd_regional_partner_mappings rpm 
    on rpm.state = sa.state_abbreviation 
        or rpm.zip_code = ssz.zip 
    left join regional_partners rp  
    on rpm.regional_partner_id = rp.regional_partner_id 
    where tt.workshop_id is null   -- prevents double counting teachers and workshops that are recorded in pd_workshops *and* sections
       
)

select * from pd_workshop_based

union all

select * from sections_based