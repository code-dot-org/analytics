with active_teachers as (
    select * from {{ref('dim_active_teachers')}}
    where user_type_merged <> 'student'
)
, final as (
    select
        event_date_merged       as event_date,
        us_intl_merged          as us_intl,
        case when user_type_merged = 'teacher' then 'cdo teacher' 
            when user_type_merged = 'amp user' then 'anonymous' 
            else 'unexpected user type ' || user_type_merged 
        end as user_type,
        known_cdo_user,
        count(distinct( user_id_merged)) as num_teachers
    from active_teachers
    group by 1,2,3,4
)
select *
from final
order by event_date desc