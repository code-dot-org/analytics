with 
source as (
    select *
    from {{ ref('seed_pardot_prospects') }}
),

renamed as (
    select 
        prospect_id,
        campaign,
        source, 
        
        hour_of_code_role   as hoc_role,
        db_grades_taught    as grades_taught,
        
        case when db_opt_in = 'yes'
             then 1 else 0 end as is_opt_in,
        
        -- geographic information
        case when lower(db_country) in (
            'united states', 
            'us', 
            'usa', 
            'u.s.a',
            'united states of america')
            then 'us' else lower(db_country) 
            end as country,
        db_state as state, 
        db_city as city,
        
        -- dates
        created_date        as created_at,
        updated_date        as updated_at,
        last_activity_at,
        db_forms_submitted  as last_submitted_at
    from source )

select *
from renamed