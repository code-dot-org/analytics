/*

Note: projects differ from other records of user activity on the platform (e.g. sign_ins, user_levels) in that it does
track anonymous usage.

A project gets a storage_id associated with a user (presumably tied to browser session, but need to verify) and when a
code.org user is actually signed in there is mapping of storage_ids to user_ids (user_project_storage_ids).

In this model we create a user_id_merged that is the code.org user_id if exists and the storage_id otherwise, prepending 'storage_id_' to make it obvious.

*/
select
    upsi.user_id        as cdo_user_id, --alias to avoid someone accidentally joining to user_id without realizing this can be null in the case of anonymous users. Use user_id_merged for joins instead.
    p.storage_id,
    created_at::date    as activity_date, 

    -- storage_ids look too much like user_ids and may have collisions/false positive matches to user_id if we just did a straing coalesce.
    -- So prepend 'storage_id_' to avoid accidental joins
    coalesce(
        upsi.user_id::varchar, 'storage_id_' || p.storage_id
    ) as user_id_merged,
    case when upsi.user_id is not NULL then 1 else 0 end as known_cdo_user,
    listagg(distinct project_type, ', ') within group (
        order by project_type
    ) as project_types,
    count(*) as num_project_records

from {{ ref('stg_dashboard_pii__projects') }} as p
left join
    {{ ref('stg_dashboard__user_project_storage_ids') }} as upsi
    on p.storage_id = upsi.user_project_storage_id
where
    --remove this filter before publish, make incremental?
    created_at >= '2022-07-01'
{{ dbt_utils.group_by(5) }}
