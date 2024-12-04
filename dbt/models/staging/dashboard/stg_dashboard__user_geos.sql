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
        end as us_intl
    from {{ ref('base_dashboard__user_geos') }}
),

country_standardizations as (
    select *
    from {{ref('seed_country_standardizations')}}
),

combined as (
    select *
    from user_geos
    left join country_standardizations
        on user_geos.country = country_standardizations.country_user_geos
    where row_number = 1
),

final as (
    select
        -- pk
        user_id,

        -- geos 
        lower(city) as city,
        lower(state) as state_name,
        postal_code,
        coalesce(iso_country, lower(country)) as country,
        is_international,
        us_intl,
        
        -- dates
        created_at,
        updated_at,
        indexed_at
    from combined
)

select *
from final 