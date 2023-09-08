create procedure run_rosetta_intl()
    language plpgsql
as
$$
BEGIN

create temporary table intl_historic_teachers as (
with historic_teachers as (
select school.*, csf.csf_trained from
(select distinct u.id user_id, usi.school_info_id, si.school_id, coalesce(cc.display_name, si.country, ug.country) country, coalesce(si.school_name, s.name) as school_name  -------------  School infos table is more accurate than schools
, usi.start_date as rec_eff_dt, case when usi.end_date is not null then end_date else '9999-12-31' end as rec_cncl_dt 
from dashboard_production.users u 
	join dashboard_production_pii.user_school_infos usi on u.id = usi.user_id and u.user_type = 'teacher' 
	join dashboard_production.user_geos ug on u.id = ug.user_id 
	left join dashboard_production.school_infos si on si.id = usi.school_info_id
	left join dashboard_production.schools s on si.school_id = s.id
	left join (select distinct country_cd, display_name from public.countries) cc on si.country = cc.country_cd 
	where coalesce(cc.display_name, si.country, ug.country) <> 'United States'
	and coalesce(cc.display_name, si.country, ug.country) is not null) school
left join (select user_id, min(workshop_date) csf_trained from analysis.international_teacher_roster group by 1) csf on school.user_id = csf.user_id
)

select teacher_user_id, country, school_info_id, school_id, school_name, ht.cal_year, csf_trained, csf_started, csd_started, csp_started
from
	(select ht.user_id as teacher_user_id, ht.country, school_info_id, school_id, school_name, sy.cal_year,
	case when csf_trained < add_months(sy.ended_at,-1) then 1 else 0 end as csf_trained, ---------------- May instead of June for CSF
	case when csf.user_id is not null then 1 else 0 end as csf_started, 
	case when csd.user_id is not null then 1 else 0 end as csd_started, 
	case when csp.user_id is not null then 1 else 0 end as csp_started,
	rank () over (partition by ht.user_id, sy.cal_year order by school_id, ht.rec_cncl_dt desc) rnk
	from historic_teachers ht
	join analysis.years sy on ht.rec_eff_dt < sy.ended_at and ht.rec_cncl_dt > sy.started_at and sy.started_at < current_date
	left join (select distinct user_id, extract(year from started_at) cal_year from analysis.csp_csd_started_teachers where course_name = 'csp') csp on ht.user_id = csp.user_id and sy.cal_year = csp.cal_year
	left join (select distinct user_id, extract(year from started_at) cal_year from analysis.csp_csd_started_teachers where course_name = 'csd') csd on ht.user_id = csd.user_id and sy.cal_year = csd.cal_year
	left join (select distinct user_id, extract(year from started_at) cal_year from analysis.csf_started_teachers) csf on ht.user_id = csf.user_id and sy.cal_year = csf.cal_year
	) ht
where rnk = 1);

drop table public.intl_historic_teachers;
create table public.intl_historic_teachers as 
(select * from intl_historic_teachers);
grant select on public.intl_historic_teachers to group reader_pii;

/*
BAKER NOTE: 03.06.23

The same speedup done for rosetta_v2 in construction of ul_start, ul_end and ul_days can be
reused here for int'l code.  See notes for similar thing done in rosetta_v1, and the logic
in rosetta_v2. CONSEQUENCE: rosetta_intl is dependent on rosetta_v2 running first.

NOTE 3.6.23: upon examination, ul_days (see below) does have logic to limit outside the US. It's probably
possible to easily speed it up a la rosetta_v1 and v2 but not taking the time to worry about that now

*/
-- DROP TABLE IF EXISTS ul_start;
-- DROP TABLE IF EXISTS ul_end;
create temporary table ul_start_intl as (
  SELECT 
  ul.*,
  y.cal_year --add cal_year field to ul_start so it plays nice with intl stuff
  FROM analysis.rosetta_ul_start_end_stable ul 
  JOIN analysis.years y on ul.act_dt between y.started_at and y.ended_at
  WHERE rnk_start=1
);
  
create temporary table ul_end_intl as (
SELECT
  ul.*,
  y.cal_year

FROM analysis.rosetta_ul_start_end_stable ul 
JOIN analysis.years y on ul.act_dt between y.started_at and y.ended_at

WHERE rnk_end=1

);


/*  ---- OLD CODE for ul_start, ul_end replaced by the above ---- 
-- drop table ul_start;
create temporary table ul_start as (
select user_id, cal_year, created_at act_dt, course_name, script_id, stage_id, level_id from (
	select ul.user_id, sy.cal_year, ul.created_at, cs.course_name_true as course_name, cs.script_id, cs.stage_id, cs.level_id, rank () over (partition by user_id, cal_year, course_name_true order by ul.created_at, level_script_order, level_number, ul.script_id, cs.stage_id) rnk  -----------------script id ordering 
	from dashboard_production.user_levels ul 
	join analysis.course_structure cs ----------------  Inner join because we don't care about levels not in course_structure
	on ul.script_id = cs.script_id and ul.level_id = cs.level_id 
	join analysis.years sy on ul.created_at between sy.started_at and sy.ended_at --------------  In order to select one record per user per course per year we need to attach the activty to a school_year
	where ul.attempts > 0
	) 
where rnk = 1
);



-- drop table ul_end;
create temporary table ul_end as (
select user_id,cal_year , created_at act_dt, course_name, script_id, stage_id, level_id from (
	select ul.user_id, sy.cal_year, ul.created_at, cs.course_name_true as course_name, cs.script_id, cs.stage_id, cs.level_id, rank () over (partition by user_id, cal_year, course_name_true order by ul.created_at desc, level_script_order desc,level_number desc, ul.script_id desc, cs.stage_id desc ) rnk  -----------------script id ordering 
	from dashboard_production.user_levels ul 
	join analysis.course_structure cs 
	on ul.script_id = cs.script_id and ul.level_id = cs.level_id 
	join analysis.years sy on ul.created_at between sy.started_at and sy.ended_at
	where ul.attempts > 0
	) 
where rnk = 1
);
*/

--drop table if exists student_sections;
create temporary table student_sections_intl as (
select distinct cal_year, student_user_id, user_id as teacher_user_id, section_id from
	(select sy.cal_year, f.student_user_id, s.user_id, s.id section_id, rank() over (partition by student_user_id, cal_year order by f.created_at desc, s.first_activity_at desc, s.course_id desc, s.script_id desc) rnk  ------------- preferred order assumption
	from dashboard_production.followers f
	join dashboard_production.sections s on f.section_id = s.id  -- and first_activity_at <> '1970-01-01 00:00:00'
	join analysis.years sy on f.created_at between sy.started_at and sy.ended_at
) s
where rnk = 1
);  ---------------------------  only assigns one section per user per year



--  drop table if exists students;
create temporary table students_intl as (
select distinct st.user_id as student_user_id, st.cal_year, st.course_name,
case when u.gender = 'm' then 'm'
	when u.gender = 'f' then 'f'
	when u.gender in ('o','n') then 'n'
	else null 
	end as gender, 
u.urm, datediff(year, u.birthday, ((st.cal_year)::int||'-07-01')::date) as age,
 ht.school_id, ht.school_info_id, ht.school_name, sts.teacher_user_id, sts.section_id, 
case when st.course_name = 'csf' and csf_trained = 1 then 1
when st.course_name = 'hoc' and csf_trained = 1 then 1
else 0 end as teacher_trained
from ul_start_intl st
join ul_end_intl en on st.user_id = en.user_id and st.cal_year = en.cal_year and st.course_name = en.course_name
join dashboard_production_pii.users u on st.user_id = u.id  and u.user_type = 'student'
join dashboard_production.user_geos ug on u.id = ug.user_id and ug.country <> 'United States' and ug.country is not null  -------------------------------  
left join student_sections_intl sts on st.user_id = sts.student_user_id and st.cal_year = sts.cal_year
left join intl_historic_teachers ht on ht.teacher_user_id = sts.teacher_user_id and ht.cal_year = st.cal_year
);

drop table public.intl_historic_students;
create table public.intl_historic_students as 
(select * from students_intl);
grant select on public.intl_historic_students to group reader_pii;

--drop table if exists ul_days;
create temporary table ul_days_intl as (
select ul.user_id, -- rhsw.section_id, 
course_name_true as course_name, cal_year, to_date(ul.created_at, 'yyyy-mm-dd') dt, count(distinct ul.level_id) all_levels_touched, sum(time_spent) time_spent,
count(distinct (case when l.type in ('CurriculumReference','StandaloneVideo','FreeResponse','External','Map','LevelGroup') then Null --------- UPDATE THIS WHEN LEVEL_TYPES CHANGE!!!!!  NO CSF LEVELS ARE CURRENTLY INCLUDED
else ul.level_id end)) course_progress_levels_touched
from dashboard_production.user_levels ul   
join analysis.course_structure cs on ul.level_id = cs.level_id and ul.script_id = cs.script_id --  limit to decided level types
join dashboard_production.user_geos ug on ul.user_id = ug.user_id and ug.country <> 'United States' and ug.country is not null  --  US only
join dashboard_production_pii.users u on ul.user_id = u.id and u.user_type = 'student' ---- filter to students only
join analysis.years sy on ul.created_at between sy.started_at and sy.ended_at 
join dashboard_production.levels l on l.id = ul.level_id
where ul.attempts > 0
group by 1,2,3,4
);



-- drop table user_days;
--drop table if exists user_days;
create temporary table user_days_intl as
(select ul.user_id, ul.cal_year, rhsw.section_id, u.races, ul.course_name, count(distinct dt) unique_days, sum(all_levels_touched) all_levels_touched, sum(course_progress_levels_touched) course_progress_levels_touched, sum(time_spent) time_spent, min(dt) first_touch, max(dt) last_touch
from ul_days_intl ul
join students_intl rhsw on ul.user_id = rhsw.student_user_id and ul.course_name = rhsw.course_name and ul.cal_year = rhsw.cal_year
join dashboard_production_pii.users u on u.id = ul.user_id
group by 1,2,3,4,5
);



--drop table if exists user_activity_stats;
create temporary table user_activity_stats_intl as (
select distinct
s.student_user_id, s.cal_year, s.course_name, s.section_id,
ud.unique_days, --- Days the user touched any level in the course
ud.all_levels_touched, --------  all levels from the course the user touched 
ud.course_progress_levels_touched,  -------  all levels of the types defined in ul_days "legit course progress" levels the user completed  (will soon contain logic to limit to section date range)
ud.time_spent,   ---------------  The "time spent" field in user_levels
ud.first_touch,
ud.last_touch,
st.script_id as start_script_id,
st.stage_id as start_stage_id,
st.level_id as start_level_id,
en.script_id as end_script_id,
en.stage_id as end_stage_id,
en.level_id as end_level_id
from students_intl s
join ul_start_intl st on st.user_id = s.student_user_id and st.cal_year = s.cal_year and st.course_name = s.course_name 
join ul_end_intl en on st.user_id = en.user_id and st.cal_year = en.cal_year and st.course_name = en.course_name 
join user_days_intl ud on s.student_user_id = ud.user_id and s.course_name = ud.course_name and s.cal_year = ud.cal_year and coalesce(ud.section_id,1) = coalesce(s.section_id,1)
);


drop table public.intl_user_activity_stats;
create table public.intl_user_activity_stats as 
(select * from user_activity_stats_intl);
grant select on public.intl_user_activity_stats to group reader_pii;

--drop table if exists section_date_range;
create temporary table section_date_range_intl as (	
select section_id, cal_year, course_name,
	avg_unique_days, users, avg_levels_touched,
	dateadd(day,first_touch,(cal_year||'/01/01')::date) first_touch,
	dateadd(day,last_touch,(cal_year||'/01/01')::date) last_touch,
	rank () over (partition by section_id, cal_year order by avg_levels_touched desc, avg_unique_days desc, last_touch desc) course_rnk,
	count(course_name) over (partition by section_id, cal_year) courses_in_section
from
	(select section_id, cal_year, course_name,
		avg(unique_days) avg_unique_days,
		avg(datediff(day,(cal_year||'/01/01')::date,first_touch)) first_touch,
		avg(datediff(day,(cal_year||'/01/01')::date,last_touch)) last_touch,
		count(distinct student_user_id) users,
		avg(all_levels_touched) avg_levels_touched
	from user_activity_stats_intl
	where all_levels_touched >= 5
	group by 1,2,3)
where users >= 5 
and section_id is not null);



-- drop table section_stats
create temporary table section_stats_intl as (
select section_id, cal_year, course_name, avg_levels_touched, avg_unique_days, users, first_touch, last_touch, courses_in_section, 
	case when course_rnk = 1 then 1 else 0 end as primary_section_course
from
	section_date_range_intl);

drop table public.intl_section_stats;
create table public.intl_section_stats as 
(select * from section_stats_intl);
grant select on public.intl_section_stats to group reader_pii;

END;
$$;

grant execute on procedure run_rosetta_intl() to baker;

grant execute on procedure run_rosetta_intl() to group admin;

