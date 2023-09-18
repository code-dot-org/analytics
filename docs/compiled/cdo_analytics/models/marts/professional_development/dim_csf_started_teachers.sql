with 
course_structure as (
    select * 
    from "dev"."dbt_allison"."dim_course_structure"
),

user_levels as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__user_levels"
),

users as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__users"
),

school_years as (
    select * 
    from "dev"."dbt_allison"."int_school_years"
),

sections as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__sections"
),

followers as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__followers"
),

csf_started as ( 
  select cs.script_id,
  coalesce(cs.script_name_short, cs.script_name) script_name,
  ul.user_id,
  sy.school_year,
  min(ul.created_at)::date started_at,
  max(ul.updated_at)::date last_progress_at
from course_structure cs
  join user_levels ul on ul.script_id = cs.script_id and ul.level_id = cs.level_id 
  join users u on u.user_id = ul.user_id and u.user_type = 'student'
  join school_years sy on ul.created_at between sy.started_at and sy.ended_at
where cs.course_name_true = 'csf'
  and ul.created_at is not null
  and ul.attempts > 0
group by 1,2,3,4
),

csf_temp as (
    select 
    se.user_id,
    st.script_id, 
    st.script_name,
    sy.school_year,
    st.started_at::date,
    st.last_progress_at::date as last_progress_at,
    row_number() over(partition by st.script_id, se.user_id, sy.school_year order by st.started_at asc) started_at_order
  from csf_started st
    join school_years sy on st.started_at between sy.started_at and sy.ended_at
    join followers f on f.student_user_id = st.user_id and f.created_at between sy.started_at and sy.ended_at
    join sections se on se.section_id = f.section_id
)

select 
  user_id,
  script_id,
  script_name,
  school_year, 
  max(case when started_at_order = 5 then started_at else null end) as started_at,
  max(case when started_at_order >= 5 then last_progress_at else null end) as last_progress_at
from csf_temp
group by 1,2,3,4
having max(started_at_order) >= 5