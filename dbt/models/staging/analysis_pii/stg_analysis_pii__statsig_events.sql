with 
statsig_events as (
    select user_id,
        stable_id,	
        custom_ids,	
        timestamp   as event_at,
        lower(event_name) as event_name,	
        lower(event_value) as event_value,	
        
        -- extract SUPER field data
        cast(user_object.country as varchar)        as country,	
        cast(company_metadata.user_type as varchar) as user_type,
        cast(company_metadata.lab_type as varchar)  as lab_type,
        cast(company_metadata.levelId as varchar)   as level_id,
        cast(company_metadata.scriptId as varchar)  as script_id,
        cast(company_metadata.sectionId as varchar) as section_id,
        cast(company_metadata.unitId as varchar)    as unit_id,
        cast(company_metadata.unitName as varchar)  as unit_name,
        cast(company_metadata.pageUrl as varchar)   as page_url 
    from {{ ref('base_analysis_pii__statsig_events') }} 
),

renamed as (
    select 
        user_id,
        -- stable_id,	
        custom_ids,	
        event_at,
        event_name,	
        event_value,	

        -- clean fields extracted from SUPER column
        lower(trim(both '"' from country))     as country,	
        lower(trim(both '"' from user_type))   as user_type,
        lower(trim(both '"' from lab_type))    as lab_type,
        lower(trim(both '"' from level_id))    as level_id,
        lower(trim(both '"' from script_id))   as script_id,
        lower(trim(both '"' from section_id))  as section_id,
        lower(trim(both '"' from unit_id))     as unit_id,
        lower(trim(both '"' from unit_name))   as unit_name,
        lower(trim(both '"' from page_url))    as page_url
        
    from statsig_events )

select * 
from renamed
