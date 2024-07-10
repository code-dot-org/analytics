with amp_events as (
    select * 
    from {{ ref('base_amplitude__active_teacher_events') }}
)
, final as (
    select
        -- user ids and event time and type
        amplitude_id,
        ltrim(user_id::varchar, 0) as cdo_user_id,  -- Amplitude adds leading zeros to user_ids less than 5 chars long, strip them. (good find by natalia)
        event_id, -- I don't know what this is, yet
        event_time,
        event_type,

        case
            when event_type = 'Teacher Viewing Student Work' then 'View Work'
            when event_type = 'Section Progress Viewed' then  'View Progress' --this version is undercounting, updated by platform to version below
            when event_type = 'Accurate V1 Section Progress Viewed' then 'View Progress'
            when event_type = 'Teacher Login' then 'Login Page'
            when event_type = 'Unit Overview Page Visited By Teacher' then 'View Unit Page'
            when event_type = 'Lesson Overview Page Visited' then 'View Lesson Plan'
            when event_type = 'Section Progress Unit Changed' then 'Toggle Unit'  --Better captures what's happening, per platform
            when event_type = 'Section Curriculum Assigned' then 'Assign Unit'
            else event_type     
        end as event_type_short_name,
            
        -- json blobs with data and info
        data,
        event_properties,
        user_properties,

        -- user's system/device info
        device_family,
        device_type,
        os_name,

        -- region location info
        dma,
        city,
        region, --state, if US
        lower(country) as country,      -- lower to align with user_geos
        case 
            when lower(country) = 'united states' then 'us' 
            when lower(country) <> 'united states' then 'intl'
            else null
        end us_intl,
        language
    from amp_events
)
select *
from final