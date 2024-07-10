/*
    This model summarizes user_level activity per user per day.
*/
select
    ul.user_id,
    ul.created_at::date activity_date,
    --ul.updated_at::date       -- would be better if we could log updated_at for daily activity

    listagg(distinct cs.course_name_true) within group (order by cs.course_name_true) course_list,
    count(*) num_user_level_records

from {{ ref('stg_dashboard__user_levels') }} ul
left join {{ ref('dim_course_structure') }} cs 
    on cs.level_id = ul.level_id 
    and cs.script_id = ul.script_id
where
    trunc(ul.created_at) > '2022-01-01' --remove this filter before publish, make incremental?
    and ul.created_at <= (select max(created_at) from {{ ref('dim_users')}}) -- don't pull records for which we don't have user_ids (this will all be records and accounts created within the last ~24 hours or so)
group by 1,2
