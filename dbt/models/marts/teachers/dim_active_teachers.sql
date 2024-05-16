with all_teacher_sign_ins as (

    select
        si.sign_in_at::date as sign_in_date,
        si.user_id::varchar,
        u.user_type,
        u.country,
        u.us_intl,
        sum(si.sign_in_count) as num_sign_in_count,
        count(*) as num_records

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
        a.cdo_user_id::varchar  as amp_cdo_user_id, -- Amplitude adds leading zeros to user_ids less than 5 chars long, strip them. (good find by natalia)


        -- cdo info, if exists
        ug.country      as cdo_country,
        ug.us_intl      as cdo_us_intl,
        u.user_type     as cdo_user_type,

        -- concatenate distinct events list
        listagg(distinct a.event_type_short_name, ', ') within group (
            order by a.event_type_short_name
        )  as events,

        max(a.country)               as amp_country, -- when aggregating to a single date, it turns out amplitude users end up being reported in multiple countries per day (probably due to VPN?) so just pick one
        max(a.us_intl)               as amp_us_intl, -- ibid. taking the max of both a.country and a.us_intl should pick the same record so country matches us_intl designation
        count(*)                     as num_amp_records

    from {{ ref('stg_amplitude__active_teacher_events') }} as a
    left join {{ ref('stg_dashboard_pii__users') }} as u on a.cdo_user_id = u.user_id
    left join {{ ref('stg_dashboard__user_geos') }} as ug on a.cdo_user_id = ug.user_id
    where event_time between '2024-01-01' and sysdate
    {{dbt_utils.group_by(6)}}

),
final as (
    select

        coalesce(sign_in_date, amp_event_date) as event_date_merged,

        -- coalesce values for each user|day between cdo sign in and amplitude event 3 possible values
        -- giving preference in the following order:
        -- 1. the cdo values from signin event
        -- 2. the cdo values from the amplitude event joined on amplitude's capture of cdo user_id
        -- 3. amplitude version of the value - id, user_type, us_intl (country)
        coalesce(si.user_id, amp_cdo_user_id, a.amplitude_id::varchar) as user_id_merged,
        coalesce(si.us_intl, a.cdo_us_intl, a.amp_us_intl) as us_intl_merged,
        coalesce(si.user_type, a.cdo_user_type, 'amp user') as user_type_merged,
        coalesce(si.country, a.cdo_country, a.amp_country) as country_merged,

        -- user ids from various sources
        si.user_id,
        a.amp_cdo_user_id user_id_amp,
        a.amplitude_id,

        -- hunam readable shorthand - where is the user_id coming from? 
        case
            when si.user_id is not NULL then 'cdo'          -- cdo sign-in only
            when amp_cdo_user_id is not NULL then 'ampcdo'  -- amplitude's cdo user_id capture
            when user_id_merged = amplitude_id then 'anon'  -- amplitude_id == anonymous
            else 'Unexpected user_id source' -- fail loudly in the case of an exceptional event
        end as merged_user_id_source,


        -- geographic info from vaious sources
        si.country as country,
        a.cdo_country as country_amp_cdo,
        a.amp_country as country_amp,
        
        si.us_intl,
        a.cdo_us_intl as us_intl_amp_cdo,
        a.amp_us_intl as us_intl_amp,

        -- user type info 
        si.user_type,
        a.cdo_user_type as user_type_amp_cdo,


        -- info about the amplitude events and sign-ins
        a.events as events_list_amp,
        case when si.num_records is not NULL then
            case when a.events is not NULL and a.events <> '' then a.events || ', cdo_sign_in' else 'cdo_sign_in' end
        else a.events
        end as events_list_merged,
        si.num_sign_in_count as cdo_sign_in_count,
        coalesce (a.num_amp_records, 0) as amp_num_records,
        coalesce (si.num_records, 0) as cdo_num_records,

        -- useful binary flags 
        coalesce (si.user_id is not NULL or amp_cdo_user_id is not NULL, false) as known_cdo_user,
        coalesce (a.num_amp_records is not NULL, false) as has_amp_event,
        coalesce (si.num_records is not NULL, false) as has_cdo_sign_in

    from all_teacher_sign_ins as si
    full outer join
        all_teacher_amp_events as a
        on si.sign_in_date = a.amp_event_date and si.user_id = a.amp_cdo_user_id
)

select *
from final
