with all_teacher_sign_ins as (

    select
        si.sign_in_at::date as sign_in_day,
        si.user_id::varchar,
        u.user_type,
        ug.country,
        ug.us_intl,
        sum(si.sign_in_count) as num_sign_in_count,
        count(*) as num_records

    from {{ ref('stg_dashboard__sign_ins') }} as si
    left join {{ ref('stg_dashboard__users') }} as u on si.user_id = u.user_id
    left join {{ ref('stg_dashboard__user_geos') }} as ug on si.user_id = ug.user_id

    -- start at school year 23-24 because amplitude didn't start until 1.1.24
    where sign_in_at >= '2023-07-01' and u.user_type = 'teacher'
    group by 1, 2, 3, 4, 5
),

event_short_names as (
    select
        'Teacher Viewing Student Work' as event_type,
        'View Work' as event_name_short
    union all
    select
        'Section Progress Viewed',
        'View Progress'
    union all
    select
        'Teacher Login',
        'Login Page'
    union all
    select
        'Unit Overview Page Visited By Teacher',
        'View Unit Page'
    union all
    select
        'Lesson Overview Page Visited',
        'View Lesson Plan'
    union all
    select
        'Section Progress Unit Changed',
        'Change unit'
),

all_teacher_amp_events as (
    select
        --amplitude info
        a.event_time::date as amp_event_day,
        a.amplitude_id,
        ltrim(a.cdo_user_id::varchar, 0) as amp_cdo_user_id, -- Amplitude adds leading zeros to user_ids less than 5 chars long, strip them. (good find by natalia)
        a.country as amp_country,
        a.us_intl as amp_us_intl,

        -- cdo info, if exists
        ug.country as cdo_country,
        ug.us_intl as cdo_us_intl,
        u.user_type as cdo_user_type,

        -- concatenate distinct events list
        listagg(distinct e.event_name_short, ', ') within group (
            order by e.event_name_short
        ) as events,
        count(*) as num_amp_records

    from {{ ref('stg_amplitude__active_teacher_events') }} as a
    inner join event_short_names as e on a.event_type = e.event_type
    left join {{ ref('stg_dashboard_pii__users') }} as u on a.cdo_user_id = u.user_id
    left join {{ ref('stg_dashboard__user_geos') }} as ug on a.cdo_user_id = ug.user_id
    where event_time > '2023-12-31'
    group by 1, 2, 3, 4, 5, 6, 7, 8

),
data_set as (
    select

        coalesce(sign_in_day, amp_event_day) as event_date_merged,

        -- coalesce values for each user|day between cdo sign in and amplitude event 3 possible values.
        -- Giving preference in the following order:
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

        -- segment: where is the user_id coming from? 
        case
            when si.user_id is not NULL then 'cdo'          -- cdo sign-in only
            when amp_cdo_user_id is not NULL then 'ampcdo'  -- amplitude's cdo user_id capture
            when user_id_merged = amplitude_id then 'anon'  -- amplitude_id == anonymous
            else 'ERROR' -- fail loudly - this should never happen
        end as merged_user_id_source,


        -- geographic info from vaious sources
        si.country as country,
        a.cdo_country as country_amp_cdo
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
        on si.sign_in_day = a.amp_event_day and si.user_id = a.amp_cdo_user_id
)

select * from data_set
