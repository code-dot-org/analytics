with 

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

teachers as (
    select teacher_id 
    from {{ ref('dim_teachers') }}
),

statsig_events as (
    select * 
    from {{ ref('stg_analysis_pii__statsig_events') }}
    where user_id in (select teacher_id from teachers)
    and event_name in (
        'curriculum catalog visited',
        'level activity',
        'lesson overview page visited',
        'unit overview page visited by teacher',
        'lesson resource link visited',
        'teacher viewing student work',
        'section setup completed',
        'section curriculum assigned',
        'teacher viewing student work',
        'section progress viewed',
        'level feedback submitted',
        'rubric activity'
    )
),

activity_levels as (
    select 
        user_id                                 as teacher_id,
        trunc(event_at)                         as activity_date,
        event_name,
        case
            when event_name in (
                'curriculum catalog visited',
                'level activity'
            )
            then 1
            else 0
        end                                     as has_light_activity,

        case
            when event_name in (
                'lesson overview page visited',
                'unit overview page visited by teacher',
                'lesson resource link visited',
                'teacher viewing student work',
                'section setup completed',
                'section curriculum assigned',
                'teacher viewing student work'
            )
            then 1
            else 0
        end                                     as has_moderate_activity,

        case
            when event_name in (
                'section progress viewed',
                'level feedback submitted',
                'rubric activity'
            )
            then 1
            else 0
        end                                     as has_heavy_activity

    from statsig_events
    group by 1,2,3,4
)

select distinct
    teacher_id, 
    activity_date,
    school_years.school_year,
    extract(year from activity_date)                 as cal_year,
    case 
        when sum(has_heavy_activity) >= 1
        then 'heavy'
    else 
        case 
            when sum(has_moderate_activity) >= 1
            then 'moderate'
        else 
            case
                when sum(has_light_activity) >= 1
                then 'light'
            else null 
            end
        end
    end                                             as activity_type,

    case 
        when sum(has_light_activity) >= 1
        then 1 
        else 0 
    end                                             as has_light_activity,
    
    case 
        when sum(has_moderate_activity) >= 1
        then 1 
        else 0 
    end                                             as has_moderate_activity,

    case 
        when sum(has_heavy_activity) >= 1
        then 1 
        else 0 
    end                                             as has_heavy_activity,
    
    listagg(distinct event_name, ', ') 
            within group (
                order by teacher_id, activity_date
            )                                       as events_touched

from activity_levels 
join school_years 
    on activity_levels.activity_date between school_years.started_at and school_years.ended_at
group by 1,2,3,4