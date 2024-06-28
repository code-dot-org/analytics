with all_teacher_sign_ins as (

    select
        si.sign_in_at::date as sign_in_date,
        si.user_id::varchar as cdo_user_id,
        u.user_type,
        u.country,
        u.us_intl,
        'cdo_sign_in'   as event_type,
        count(*)        as num_records

    from {{ ref('stg_dashboard__sign_ins') }} as si
    left join {{ ref('dim_users') }} u on u.user_id = si.user_id

    -- start at school year 23-24 because amplitude didn't start until 1.1.24
    where si.sign_in_at >= '2023-07-01' and u.user_type = 'teacher'
    {{dbt_utils.group_by(5)}}
),

all_teacher_amp_events as (
    select
        --amplitude info
        a.event_time::date      as amp_event_date,
        a.amplitude_id,
        a.cdo_user_id::varchar  as amp_cdo_user_id,

        -- cdo info, if exists
        u.country      as cdo_country,
        u.us_intl      as cdo_us_intl,
        u.user_type     as cdo_user_type,

        -- concatenate distinct events list
        listagg(distinct a.event_type_short_name, ', ') within group (
            order by a.event_type_short_name
        )  as events,

        max(a.country)               as amp_country, -- when aggregating to a single date, it turns out amplitude users end up being reported in multiple countries per day (probably due to VPN?) so just pick one
        max(a.us_intl)               as amp_us_intl, -- ibid. taking the max of both a.country and a.us_intl should pick the same record so country matches us_intl designation
        count(*)                     as num_amp_records

    from {{ ref('stg_amplitude__active_teacher_events') }} as a
    left join {{ ref('dim_users')}} u on u.user_id = a.cdo_user_id
    where event_time between '2024-01-01' and sysdate
    {{dbt_utils.group_by(6)}}

),
unioned_sets as (

    select
        sign_in_date    as event_date,
        'cdo'           as event_source,
        cdo_user_id     as cdo_user_id,
        null            as amplitude_id,
        event_type      as events,
        num_records     as num_records,
        user_type       as user_type,
        country         as country,
        us_intl         as us_intl

    from all_teacher_sign_ins

    union all

    -- amplitude events with a known code.org user (using cdo geo info)
    select
        amp_event_date      as event_date,
        'amp'               as event_source,
        amp_cdo_user_id     as cdo_user_id,
        amplitude_id        as amplitude_id,
        events              as events,
        num_amp_records     as num_records,
        cdo_user_type       as user_type,
        cdo_country         as country,
        cdo_us_intl         as us_intl
    from all_teacher_amp_events
    where amp_cdo_user_id IS NOT NULL -- if there exists a cdo user_id then it's a known cdo user

    union all

    -- amplitude events that are anonymous, use amplitude user_geo info
    select
        amp_event_date      as event_date,
        'amp'               as event_source,
        null                as cdo_user_id,
        amplitude_id        as amplitude_id,
        events              as events,
        num_amp_records     as num_records,
        'anon'              as user_type,
        amp_country         as country,
        amp_us_intl         as us_intl
    from all_teacher_amp_events
    where amp_cdo_user_id IS NULL -- if no cdo user id, it's anonymous
)
, final as (

    select
        event_date              as event_date,
        case 
            when cdo_user_id is not null then cdo_user_id::varchar 
            else amplitude_id::varchar
        end                     as merged_user_id,
        max(cdo_user_id)        as cdo_user_id,
        max(amplitude_id)       as amplitude_id,
        max(user_type)          as user_type,  --this shouldn't conflict. i.e it should never be the case that this is chosing one of 'teacher' or 'anon'. They way we are aggregating it will be one or the other for this date/user combo. I have proved this experimentally.
        max(country)            as country,
        max(us_intl)            as us_intl,
        sum(case when event_source = 'cdo' then num_records else 0 end) as num_cdo_records,
        sum(case when event_source = 'amp' then num_records else 0 end) as num_amp_records,
        sum(num_records)        as num_records,

        listagg(distinct event_source::varchar, ', ') 
        within group (order by event_source)        as event_sources,

        listagg(distinct events::varchar, ', ') 
        within group (order by event_source)        as events_list

    from unioned_sets
    group by 1,2
)
select * 
from final
-- choice: for now I'm excluding known student users from the model entirely even though the prototype included them
where user_type <> 'student' 
