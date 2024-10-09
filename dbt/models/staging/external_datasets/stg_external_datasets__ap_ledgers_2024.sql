/* 
    
    Issues
    - non standard exam names
    - non standard zip/state/district_id
    - Dropping district and district ID
    - State sometimes contains country info
    
    The country filter isn't perfect - some Canada is still slipping in as US because 'CA' is listed as state, for instance. Better to use the NCES IDs whenever possible.

    We should use address + zip from NCES data instead
*/ 

with all_ledgers AS (
    select
        cast(exam_year as varchar) as exam_year,
        school_year,
        case when exam='Computer Science Principles' then 'csp'
            when exam= 'Computer Science A' then 'csa'
            end as exam,
        cast(ai_code as varchar) as ai_code,
        school_name as school_name,
        city as city,
        state as state,
        case when state in ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL',
                            'GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
                            'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH',
                            'NJ','NM','NY','NC','ND','OH','OK','OR','PA','PR',
                            'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
                            'WI','WY') then 'US' 
                    else NULL 
                    end as country,
        case when country = 'US' then zip
                    else NULL
                    end as zip,
        case when provider_syllabus = '' then Null 
            when provider_syllabus = 'Code.org + CMU CS Academy Sample Syllabus' then 'Code.org + CMU CS Academy'
            when provider_syllabus = 'Code.org Sample Syllabus' then 'Code.org' 
            when provider_syllabus = 'Code.org Sample Syllabus (For Pilot Teachers Only)' then 'Code.org' 
            end as provider_syllabus
    from {{source('external_datasets','stg_ap_ledgers_raw_2024')}}
),

national_group AS (
    select exam_year,
    school_year,
    exam,
    'national' as ledger_group,
    ai_code,
    school_name,
    city,
    state,
    country,
    NULL as provider_syllabus
    from all_ledgers
),

cdo_audit_group AS (
    select exam_year,
    school_year,
    exam,
    'cdo_audit' as ledger_group,
    ai_code,
    school_name,
    city,
    state,
    country,
    provider_syllabus
    from all_ledgers
    where provider_syllabus in ('Code.org', 'Code.org + CMU CS Academy')
)

select *
from cdo_audit_group
UNION ALL 
select *
from national_group
