/* 
    
    Issues
    - non standard exam names
    - non standard zip/state/district_id
    - Dropping district and district ID
    - State sometimes contains country info
    
    The country filter isn't perfect - some Canada is still slipping in as US because 'CA' is listed as state, for instance. Better to use the NCES IDs whenever possible.

    We should use address + zip from NCES data instead
*/ 

with new_ledgers as (
    select * 
    from {{ ref('base_external_datasets__ap_ledgers_2024') }}
    where ai_code is not null 
), 

transformed_ledger as (
    select
        cast(exam_year as varchar)                                                       as exam_year,
        school_year,
        case when exam='Computer Science Principles' then 'csp'
            when exam= 'Computer Science A' then 'csa'
            end                                                                          as exam,
        cast(ai_code as varchar)                                                         as ai_code,
        lower(school_name)                                                             as school_name,
        lower(city)                                                                      as city,
        state                                                                   as state,
        case 
            when state in (
                'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL',
                'GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
                'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH',
                'NJ','NM','NY','NC','ND','OH','OK','OR','PA','PR',
                'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
                'WI','WY'
            ) then 'us' 
            else null 
        end                                                                                 as country,
        case 
            when country = 'us' then zip
            else null
            end                                                                             as zip,
        case 
            when provider_syllabus = ''                                                     then null 
            when provider_syllabus = 'Code.org + CMU CS Academy Sample Syllabus'            then 'code.org + cmu cs academy'
            when provider_syllabus = 'Code.org Sample Syllabus'                             then 'code.org' 
            when provider_syllabus = 'Code.org Sample Syllabus (For Pilot Teachers Only)'   then 'code.org' 
            end                                                                             as provider_syllabus
    from new_ledgers
),

national_group as (
    select 
        exam_year,
        school_year,
        exam,
        'global'                                                                      as ledger_group,
        {{ pad_ai_code('ai_code') }}                                             as ai_code,
        school_name                                                             as school_name,
        city                                                                    as city,
        state                                                                    as state,
        country                                                                  as country,
        null                                                                            as provider_syllabus
    from transformed_ledger
),

cdo_audit_group as (
    select 
        exam_year,
        school_year,
        exam,
        'cdo_audit'                                                                     as ledger_group,
        {{ pad_ai_code('ai_code') }}                                               as ai_code,
        school_name                                                              as school_name,
        city                                                                     as city,
        state                                                                  as state,
        country                                                                 as country,
        provider_syllabus
    from transformed_ledger
    where provider_syllabus in (
        'code.org', 
        'code.org + cmu cs academy'
    )
),

final as (
    select *
    from cdo_audit_group

    union all 

    select *
    from national_group ) 

select * 
from final 
where ai_code is not null 