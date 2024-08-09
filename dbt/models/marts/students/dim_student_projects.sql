with 

projects as (
    select * 
    from {{ ref('stg_dashboard_pii__projects') }}
),

user_storage_ids as (
    select * 
    from {{ ref('stg_dashboard__user_project_storage_ids') }}
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
    p.project_id 
    , u.user_id
    , case 
        when u.user_id is not null then 1 
        else 0 
    end                                                                     as is_signed_in
    , u.user_type 
    , case 
        when p.published_at is not null then 1
        else 0
    end                                                                     as is_published
    , p.created_at                                                          as project_created_at
    , p.updated_at                                                          as project_updated_at
    , p.published_at                                                        as project_published_at
    , sy.school_year                                                        as school_year
    , extract('year' from p.created_at)                                     as cal_year
    , pc.display_name                                                       as country
    , ipr.region                                                            as region
    , p.is_standalone 
    , p.abuse_score 
    , p.project_type 
    , case 
        when p.state = 'deleted' then 1 
        else 0 
    end                                                                     as is_deleted
    , p.remix_parent_id
    , p.value                                                               as project_info
    , case
        when json_extract_path_text(p.value, 'id') <> '' then 1 
        else 0 
    end                                                                     as is_valid
from projects                                                               as p

join user_storage_ids                                                       as ui 
    on p.storage_id = ui.user_project_storage_id

join school_years                                                           as sy
    on p.created_at between sy.started_at and sy.ended_at

left join users                                                             as u 
    on u.user_id = ui.user_id

left join user_geos                                                         as ug 
    on u.user_id = ug.user_id

left join countries                                                         as pc 
    on pc.alt_name = ug.country

left join international_partners_raw                                        as ipr 
    on pc.display_name = ipr.display_name
