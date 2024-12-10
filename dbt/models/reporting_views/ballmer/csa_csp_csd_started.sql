with dssla as (
    select * from 
    {{ref('dim_student_script_level_activity')}}
    where user_type = 'student'
    and course_name in ('csa','csp','csd')
),

final as (
    select 
        course_name,
        student_id as user_id,
        school_year,
        min(activity_date) as started_at,
        max(activity_date) as last_progress_at,
        count(distinct level_id) as lvl_cnt
    from dssla
    group by 1,2,3
)

select * from final

/* This can be deleted before merging, just included for reference
select 
  max(course_id) course_id,
  course_name_true course_name,
  ul.user_id,
  sy.school_year,
  min(ul.created_at)::date started_at,
  max(ul.updated_at)::date last_progress_at,
  count(distinct ul.level_id) lvl_cnt
from analysis.course_structure cs
  join dashboard_production.user_levels ul on ul.script_id = cs.script_id and ul.level_id = cs.level_id 
  join dashboard_production.users u on u.id = ul.user_id and u.user_type = 'student'
  join analysis.school_years sy on ul.created_at between sy.started_at and sy.ended_at
where cs.course_name_true in ('csp','csd','csa') --Baker note 09.22.22: added 'csa' to this to support bandaid update to rosetta_historic_teachers
  and ul.created_at is not null
  and ul.attempts > 0
group by 2,3,4
*/