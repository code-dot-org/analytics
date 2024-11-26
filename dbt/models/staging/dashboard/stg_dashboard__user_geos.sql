with 
user_geos as (
    select *,
        row_number() over (partition by user_id order by user_geo_id desc) as row_number,
        case when country = 'united states' then 0
             when country <> 'united states' then 1 
             else null 
        end as is_international,
        case when country = 'united states' then 'us'
             when country <> 'united states' then 'intl'
             else null 
        end as us_intl,
        case when country = 'åland' then 'åland islands'
             when country = 'brunei' then 'brunei darussalam'
             when country = 'cape verde' then 'cabo verde'
             when country = 'cocos [keeling] islands' then 'cocos (keeling) islands'
             when country = 'czech republic' then 'czechia'
             when country = 'swaziland' then 'eswatini'
             when country = 'federated states of micronesia' then 'micronesia, federated states of'
             when country = 'ivory coast' then 'côte d''ivoire'
             when country = 'hashemite kingdom of jordan' then 'jordan'
             when country = 'republic of lithuania' then 'lithuania'
             when country = 'republic of moldova' then 'moldova'
             when country = 'myanmar [burma]' then 'myanmar'
             when country = 'the netherlands' then 'netherlands'
             when country = 'macedonia' then 'north macedonia'
             when country = 'reserved' then null
             when country = 'st kitts and nevis' then 'saint kitts and nevis'
             when country = 'st vincent and grenadines' then 'saint vincent and the grenadines'
             when country = 'slovak republic' then 'slovakia'
             when country = 'republic of korea' then 'south korea'
             when country = 'democratic republic of timor-leste' 
                or country = 'east timor' then 'timor-leste'
            when country = 'turkey' then 'türkiye'
            else country
        end as country_standardized
    from {{ ref('base_dashboard__user_geos') }}
),

final as (
    select
        -- pk
        user_id,

        -- geos 
        lower(city) as city,
        lower(state) as state_name,
        postal_code,
        lower(country_standardized) as country,
        is_international,
        us_intl,
        
        -- dates
        created_at,
        updated_at,
        indexed_at
    from user_geos
    where row_number = 1
)

select *
from final 