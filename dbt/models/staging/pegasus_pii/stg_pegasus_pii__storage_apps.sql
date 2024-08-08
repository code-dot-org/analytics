with storage_apps as (
    select *
    from {{ ref ('base_pegasus_pii__storage_apps')}}
)

select 
    id                                                  as project_id
    , storage_id
    , updated_at
    , updated_ip
    , state
    , created_at
    , abuse_score
    , project_type
    , published_at
    , standalone
    , remix_parent_id
    , skip_content_moderation
from storage_apps