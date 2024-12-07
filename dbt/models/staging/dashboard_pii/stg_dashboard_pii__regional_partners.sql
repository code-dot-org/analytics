with regional_partners as (
    select * 
    from {{ ref('base_dashboard_pii__regional_partners') }}
)

select
    regional_partner_id
    , regional_partner_group
    , lower(regional_partner_name)      as regional_partner_name
    , is_urban
    , lower(street)                     as address
    , lower(apartment_or_suite)         as apt_num
    , lower(city)                       as city
    , state
    , zip_code
    , created_at
    , updated_at
    , is_active
    , case 
        when json_extract_path_text(
            properties, 'urg_guardrail_percent'
        ) != '' 
        then json_extract_path_text(
            properties, 'urg_guardrail_percent'
        ) 
        else '50' 
    end                                                         as urg_guardrail_pct
    , case 
        when json_extract_path_text(
            properties, 'frl_guardrail_percent'
        ) != '' 
        then json_extract_path_text(
            properties, 'frl_guardrail_percent'
        ) 
        else '50' 
    end                                                         as frl_guardrail_pct

from regional_partners