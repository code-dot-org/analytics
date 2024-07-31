with 

storage_apps as (
    select * 
    from {{ ref('stg_pegasus_pii__storage_apps') }}
),

user_storage_ids as (
    select * 
    from {{ ref('stg_pegasus__user_storage_ids') }}
), 

users as (
    select * 
    from {{ ref('stg_dashboard_pii__users') }}
),

user_geos as (
    select distinct 
        user_id
        , country 
    from {{ ref('stg_dashboard__user_geos') }}
),

countries as (
    select * 
    from {{ ref('stg_public__countries') }}
),

international_partners_raw as (
    select * 
    from {{ ref('stg_public__international_partners_raw') }}
),

international_contact_info as (
    select *
    from {{ ref('stg_public__international_contact_info') }}
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
)

select distinct 
    sa.project_id                                                           as project_id
    , u.user_id                                                             as user_id
    , case 
        when u.user_id is not null then 1 
        else 0 
    end                                                                     as is_signed_in
    , u.user_type                                                           as user_type
    , case 
        when sa.published_at is not null then 1
        else 0
    end                                                                     as is_published
    , sa.created_at                                                         as project_created_at
    , case 
        when sa.published_at is not null
            then datediff(hour, sa.created_at, sa.published_at)
        else null
    end                                                                     as hours_to_publish
    , sy.school_year                                                        as school_year
    , extract('year' from sa.created_at)                                    as cal_year
    , pc.display_name                                                       as country
    , ipr.region                                                            as region
    , sa.standalone                                                         as is_standalone
    , sa.abuse_score                                                        as abuse_score
    , sa.project_type                                                       as project_type
    , sa.remix_parent_id                                                    as remix_parent_id

from storage_apps                                                           as sa

join user_storage_ids                                                       as ui 
    on sa.storage_id = ui.storage_id

join school_years                                                           as sy
    on sa.created_at between sy.started_at and sy.ended_at

left join users                                                             as u 
    on u.user_id = ui.user_id

left join user_geos                                                         as ug 
    on u.user_id = ug.user_id

left join countries                                                         as pc 
    on pc.alt_name = ug.country

left join international_partners_raw                                        as ipr 
    on pc.display_name = ipr.display_name
