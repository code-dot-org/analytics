-- version: 2.0 (JS)
-- 2024-08-30

with 
/*
    1. User Course Activity
    
    This is the mostly costly part of the model, so we will
    build it as easily as possible and earlier in the process...
    
    This first combination allows us to pull together all the user level activity and course data we need to build the rest of the model. 
    
    We can also roll through our aggregates and other calculations so as to not do them each time in some larger downstream query...
*/
user_levels as (
    select 
        user_id,
        
        -- Note: 
        --      In order to not cross-polenate too soon, this part
        --      of the model will maintain user_id only
        -- Optional: 
        --      Create surrogate key for incremental load
        --      md5 ( concat (user_id,activity_date,level_id,script_id ) as surrogate_key
        
        level_id,
        script_id,
        
        -- dates 
        date_trunc('day', created_at)    as activity_date,
        extract('month' from created_at) as activity_month,
        
        -- aggs
        max(attempts)       as total_attempts,
        max(best_result)    as best_result,
        sum(time_spent)     as time_spent_minutes
    from {{ ref('stg_dashboard__user_levels') }}
    {{ dbt_utils.group_by(5) }}
), 

course_structure as (
    select
        course_name,
        course_id,
        script_id,
        script_name,
        level_id,
        level_name,
        level_type,
        unit        as unit_name,
        stage_name  as lesson_name
    from {{ ref('dim_course_structure') }}
    
    where participant_audience = 'student' 
    -- Note: filter out data we don't want or need as early as possible. If we keep it around, it will be continuously processed as it is referenced in other queries.
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

student_activity as (
    select 
        
        ul.user_id,
        ul.level_id,
        ul.script_id,
        
        -- dates 
        sy.school_year,
        ul.activity_date,
        ul.activity_month,

        -- Note: post-process this work now that it is looking 
        -- for only a few values 
        case 
            when ul.activity_month in ( 7,   8,  9 )  then 'Q1'
            when ul.activity_month in ( 10,  11, 12 ) then 'Q2'
            when ul.activity_month in ( 1,   2,  3 )  then 'Q3'
            when ul.activity_month in ( 4,   5,  6 )  then 'Q4'
        end as activity_quarter, 

        cs.course_name,
        cs.level_name,
        cs.level_type,
        cs.script_name,
        cs.unit_name,
        cs.lesson_name,

        -- aggs
        ul.total_attempts,
        ul.best_result,
        ul.time_spent_minutes

    from user_levels as ul 
    
    left join course_structure as cs
        on ul.level_id = cs.level_id
        and ul.script_id = cs.script_id
    
    join school_years as sy
        on ul.activity_date 
            between sy.started_at 
                and sy.ended_at 
),

/* 
    2. Sections and Students
    
    We now have all that taken care of, so our join will be simpler later on (and less data in memory)

    Next, we need to map each student to their section(s). This is another big one but only bc our student_activity isn't unique to student_id... so we can pre-process out that work as well.
*/

section_mapping as (
    select *
    from {{ ref('int_section_mapping') }}
    
    where student_id in (
        select user_id 
        from student_activity )
        -- Note: again, filter out anything we don't need. We need to keep the ship as light as possible.
),

section_size as (
    select 
        section_id,
        count(distinct student_id) as section_size 
    from section_mapping
    {{ dbt_utils.group_by(1) }}
),

sections as (
    select 
        scm.*,     
        scz.section_size
    
    from section_mapping    as scm 
    join section_size       as scz 
        on scm.section_id = scz.section_id
),

/*
    So now that we have our student_activity and section mapping built, we can smash them together to get our full sections and status data 

    3. Teachers and Schools
        a. Use student_id to connect to sections
        b. Use teacher, school_id for statuses
        c. select * that shit
        d. almost forgot user_geo info 
*/

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

users as (
    select user_id, is_international, country 
    from {{ ref('dim_users') }}
),

school_status as (
    select school_id, school_year, status as school_status
    from {{ ref('dim_school_status') }}
),

teacher_status as (
    select teacher_id, school_year, status as teacher_status  
    from {{ ref('dim_teacher_status') }}
),

combined as (
    select 
        -- students
        sec.student_id,
        sec.school_year,
select 
    ul.user_id                                                      as student_id
    , ul.level_id                                                   as level_id
    , ul.script_id                                                  as script_id
    , date_trunc('day', ul.created_at)                              as activity_date
    , extract(year from ul.created_at)                              as cal_year

    -- time variables 
    --, date_trunc ('month', ul.created_at)                           as activity_month  
    , case 
        when extract ('month' from ul.created_at) in (7,8,9) then 'Q1'
        when extract ('month' from ul.created_at) in (10,11,12) then 'Q2'
        when extract ('month' from ul.created_at) in (1,2,3) then 'Q3'
        when extract ('month' from ul.created_at) in (4,5,6) then 'Q4'
      end                                                           as school_year_quarter
    , sy.school_year                                                as school_year

    -- course, script, unit, lesson, and level characteristics
    , cs.level_name                                                 as level_name
    , cs.level_type                                                 as level_type
    , cs.script_name                                                as script_name
    , cs.unit                                                       as unit_name
    , cs.course_name
    , cs.stage_name                                                 as lesson_name
        -- case when sec.student_removed_at is not null 
        --      then 1 else 0 end as is_removed,

        usr.is_international,
        usr.country,

        -- teachers
        sec.teacher_id as section_teacher_id,
        tes.teacher_status,
        
        -- sections
        sec.section_id,
        
        -- schools
        sec.school_id,
        sst.school_status,
        sch.school_name,
        sch.school_district_id,
        sch.school_district_name,
        sch.state                   as school_state,
        sch.school_type,
        sch.is_stage_el             as school_is_stage_el,
        sch.is_stage_mi             as school_is_stage_mi,
        sch.is_stage_hi             as school_is_stage_hi,
        sch.is_high_needs           as school_is_high_needs,
        sch.is_rural                as school_is_rural

        -- dates if need 

        -- sec.student_added_at,
        -- sec.student_removed_at,
        
    from sections   as sec 
    
    join teacher_status as tes 
        on tes.teacher_id   = sec.teacher_id 
        and tes.school_year = sec.school_year
    
    join school_status as sst 
        on  sec.school_id   = sst.school_id 
        and sec.school_year = sst.school_year 
    
    join schools    as sch
        on sch.school_id = sec.school_id 
    join users      as usr 
        on usr.user_id = sec.student_id
),

final as (
    select 
        comb.*,

        -- moar dates
        sta.activity_date,
        sta.activity_month,
        sta.activity_quarter, 
        
        -- coursework 
        sta.level_id,
        sta.script_id,
        sta.course_name,
        sta.script_name,
        sta.level_name,
        sta.level_type,
        sta.unit_name,
        sta.lesson_name,

        -- totals
        sta.total_attempts,
        sta.best_result,
        sta.time_spent_minutes

    from combined as comb 
    left join student_activity as sta 
        on comb.student_id = sta.user_id 
        and comb.school_year = sta.school_year )

left join school_years                                              as sy
    on ul.created_at between sy.started_at and sy.ended_at 

left join course_structure                                          as cs
    on ul.level_id = cs.level_id
    and ul.script_id = cs.script_id

left join section_mapping                                           as sm
    on ul.user_id = sm.student_id
    and sm.school_year = sy.school_year

left join section_size                                              as ssi
    on ssi.section_id = sm.section_id

left join teacher_status                                            as ts 
    on sm.teacher_id = ts.teacher_id 
    and sy.school_year = ts.school_year

left join school_status                                             as ss 
    on sm.school_id = ss.school_id 
    and sy.school_year = ss.school_year

left join schools                                                   as sch
    on sm.school_id = sch.school_id  

where cs.participant_audience = 'student'
{{ dbt_utils.group_by(30) }}

select * 
from final
