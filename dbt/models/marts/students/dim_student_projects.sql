with 
projects as (
    select * 
    from {{ ref('stg_dashboard_pii__projects') }}
),

user_storage_ids as (
    select * 
    from {{ ref('stg_dashboard__user_project_storage_ids') }}
), 

student_projects as (
    select *, 

        -- storage_ids look too much like user_ids and may have collisions/false positive matches to user_id if we just did a straing coalesce.
        -- So prepend 'storage_id_' to avoid accidental joins
        coalesce(
            ui.user_id::varchar, 
            'storage_id_' || p.storage_id
        ) as user_id_merged
        
    from projects                                                               as p
    join user_storage_ids                                                       as ui 
        on p.storage_id = ui.user_project_storage_id
),

users as (
    select * 
    from {{ ref('stg_dashboard_pii__users') }}
),

user_geos as (
    select distinct 
        user_id
        , country 
        , us_intl
    from {{ ref('stg_dashboard__user_geos') }}
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
),

final as (
    select distinct 
          p.project_id 
        , u.user_id
        , case when p.user_id   is not null then 1 else 0 end   as known_cdo_user -- (1=student, 0=anon)
        --, case when u.user_id   is not null then 1 else 0 end   as is_signed_in
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
        , ug.country                                                       as country
        , ug.us_intl                                                        as us_intl
        , p.is_standalone 
        , p.abuse_score 
        , p.project_type
        , case 
            when p.state = 'deleted' then 1 
            else 0 
        end                                                                     as is_deleted
        , p.remix_parent_id
        , p.value                                                               as project_info
        -- , case
        --     when json_extract_path_text(p.value, 'id', true) <> '' then 1 
        --     else 0 
        -- end                                                                     as is_valid

    from student_projects                                                       as p
    join school_years                                                           as sy
        on p.created_at 
            between sy.started_at 
                and sy.ended_at

    left join users                                                             as u 
        on u.user_id = p.user_id

    left join user_geos                                                         as ug 
        on u.user_id = ug.user_id
)

select * 
from final 