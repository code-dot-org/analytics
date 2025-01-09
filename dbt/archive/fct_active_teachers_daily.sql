with active_teachers as (
    select * from {{ref('dim_active_teachers')}}
    --where user_type_merged <> 'student'
)
, final as (
    select
        event_date,
        us_intl,
        user_type, -- 'teacher' = known code.org user, 'anon' means anonymous
        count(distinct( merged_user_id)) as num_teachers
    from active_teachers
    group by 1,2,3
)
select *
from final
order by event_date desc