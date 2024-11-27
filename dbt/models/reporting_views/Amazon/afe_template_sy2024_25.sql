/* This view is used for AFE's engagement reporting, following their template. Reports are delivered quarterly.
Currently we report through a Tableau Dashboard:
- AFE-facing version: https://us-east-1.online.tableau.com/#/site/codeorg/workbooks/1999033?:origin=card_share_link
- Internal version (same as AFE's plus a totals viz): https://us-east-1.online.tableau.com/#/site/codeorg/workbooks/1999068?:origin=card_share_link

The report includes the following engagement metrics, for flagship courses (CSF, CSD, CSP, CSA) and are reported separately for:
- Schools with teachers who have signed up for AFE
- Schools eligible for AFE (Title 1, % FRL-eligible students: > 40%, % URG students > 30% or rural)
- Total engagement by State

Engagement Metrics, all by course except N. Teachers:
- N. Teachers
- N. Teachers trained - In person
- N. Teachers trained - Self-paced pd
- N. Teachers trained (Both modalities)
- N. Students Started
- N. Students Committed (5+ days of activity)
- Total learning hours committed students (estimated as 1 hour per unique day of activity)
- Average learning hours committed students
*/


with

teachers as (
    select 
        t.teacher_id
        , t.teacher_email
        , t.us_intl
        , t.state
    from {{ ref('dim_teachers') }} t
    where 
    t.us_intl = 'us' -- AFE is US-based only
)

, student_script_level_activity as (
    select distinct
          sla.school_year
        , sla.section_teacher_id teacher_id
        , sla.course_name 
        , sla.school_id
        , sla.student_id
    from {{ ref('dim_student_script_level_activity') }} as sla
    where 
        sla.total_attempts > 0
        and sla.course_name  in ('csd', 'csp', 'csf','csa') 
        and sla.school_year in ('2024-25')
        and sla.user_type = 'student'
),  


student_script_level_activity_2019_20 as -- Pulling CSD and CSP teachers as in 2019 only CSD and CSP teachers received invites to join AFE
(
    select distinct
        sl.section_teacher_id as teacher_id
        , sl.school_id
    from {{ ref('dim_student_script_level_activity') }} as sl
    where 
        sl.course_name  in ('csd', 'csp') 
        and sl.school_year in ('2019-20')
)  

, user_course_activity 
as (
select 
dc.school_year
, dc.course_name 
, dc.user_id
, dc.num_unique_days
from {{ ref('dim_user_course_activity')  }} as dc
where
    dc.course_name  in ('csd', 'csp', 'csf','csa') 
    and dc.school_year in ('2024-25')
)


, pd_teachers as (
select distinct
  tt.user_id teacher_id
, tt.school_id 
, tt.course_name 
, tt.school_year
, tt.trained
from  dashboard.analysis.teachers tt 
where tt.school_year in ('2024-25') -- current year
and tt.course_name in ('csa', 'csd', 'csf', 'csp')
and tt.trained = 1 -- PD'd, facilitator-led. Trained is cumulative over time, so this brings all teachers who have been trained up to the selected school year
)


, self_paced_pd_teachers as (
select distinct
 sp.teacher_id
, sp.level_created_school_year school_year
, sp.course_name_implementation 
, sp.school_id
from {{ ref ('dim_self_paced_pd_activity') }} sp
where sp.course_name_implementation in ('csa', 'csd', 'csf', 'csp')
and sp.us_intl = 'us' -- Not filtering for a school year because we want teachers who have been PD'd ever
)

, afe_signups 
as 
(
select
  json_extract_path_text(data_json, 'accountEmail') accountEmail
, json_extract_path_text(data_json, 'accountSchoolId') accountSchoolId
, json_extract_path_text(data_json, 'formEmail') formEmail
, json_extract_path_text(data_json, 'formSchoolId') formSchoolId
, json_extract_path_text(json_extract_path_text(data_json, 'formData'), 'registration-date-time') registrationDateTime
, created_at registration_dt
from dashboard.analysis.events
where 
	study = 'amazon-future-engineer-eligibility' 
and event = 'submit_to_afe'
)


, school_stats 
as (
    select * 
    from {{ ref ('dim_schools') }}
    ) 


, teachers_students_started as 
(
select distinct
s.school_year
, s.teacher_id
, s.course_name 
, s.school_id
, s.student_id
from 
student_script_level_activity s
join teachers dt 
		on	s.teacher_id	=	dt.teacher_id -- inner join to the teacher segment defined above to limit to US-based teachers
)


, teachers_started as 
(
select 
tss.school_year
, tss.teacher_id
, tss.course_name 
, tss.school_id
, count (distinct duca.user_id) n_students_started
, count (distinct case when duca.num_unique_days > 4 then duca.user_id else null end) n_students_committed
, sum(coalesce(duca.num_unique_days,0)) hours_students_started
, sum(coalesce(case when duca.num_unique_days > 4 then duca.num_unique_days else 0 end,0)) hours_students_committed
from teachers_students_started tss
left join user_course_activity  duca 
		on	tss.student_id	=	duca.user_id
		and tss.school_year	=	duca.school_year
		and tss.course_name	= 	duca.course_name
{{ dbt_utils.group_by(4) }}
having n_students_started > 4 -- teachers with at least 5 students starting a course, regardless of the number of students in a section to allow for teachers who split students into smaller sections
)


, afe_teacher_school -- Pulls school id from AFE signups and unions with 2019-20 data which is in a different table
as 
(
select distinct
coalesce(formSchoolId,accountSchoolId) nces_id
, u.teacher_id
, ae.registration_dt
from afe_signups ae
left join teachers u 
		on u.teacher_email = (coalesce(accountEmail,formEmail))
where registrationDateTime is not null
and len((coalesce(accountEmail,formEmail)))>1
and nces_id <> -1

union all

--- School year 2019-20: only csd and csp started teachers were given an invite to AFE
select distinct
afe_19.*
, sa.teacher_id
, null::date as registration_dt
from  dashboard.public.afe_2019 afe_19
join student_script_level_activity_2019_20 sa  
		on 		 
        case
            when length(afe_19.nces_id) = 11 
        	    then lpad(afe_19.nces_id,12,0)
            when length(afe_19.nces_id) < 8
        	    then lpad(afe_19.nces_id,8,0)
            else afe_19.nces_id 
            end  
            = sa.school_id
where 
 afe_19.nces_id <> -1
)


, all_teachers -- combines all sets of teacher data: teacher starts, PD'd (in person), Self-paced PD and AFE sign-ups 
as (
select 
'2024-25'::varchar as school_year
, coalesce(ts.teacher_id,	pt.teacher_id,	afe.teacher_id, sp2.teacher_id) teacher_id
, coalesce(ts.school_id,	pt.school_id,	afe.nces_id, sp2.school_id) school_id_source
, case
        when length(school_id_source) = 11 
        	then lpad(school_id_source,12,0)
        when length(school_id_source) < 8
        	then lpad(school_id_source,8,0)
        else school_id_source
    end school_id
, coalesce(ts.course_name, pt.course_name, sp2.course_name_implementation) course_name
, decode (pt.trained
				, 1, 'Y'
				, 'N' ) trained_pd
, case 
		when ts.teacher_id is not null
		then 'Y'
		else 'N'
	end teacher_started
, case 
	when sp2.teacher_id is not null
	then 'Y'
	else 'N'end trained_self_paced_pd
, case when afe.teacher_id is not null then 'Y' else 'N' end afe_signed_up 
, afe.registration_dt 
, ts.n_students_started
, ts.n_students_committed
, ts.hours_students_started
, ts.hours_students_committed
from  teachers_started ts
full outer join pd_teachers pt -- We care about all-time PD'd, trained.analysis.teachers is cumulative 
		on  ts.teacher_id	=	pt.teacher_id 
		and ts.school_year	=	pt.school_year 
		and ts.course_name	=	pt.course_name 
full outer join self_paced_pd_teachers sp2 -- Doesn't join on school year because we care about all-time PD'd
		on   coalesce(ts.teacher_id,  pt.teacher_id)	=	sp2.teacher_id 
		and  coalesce(ts.course_name, pt.course_name)	=	sp2.course_name_implementation 
full outer join afe_teacher_school afe 
		on  coalesce(ts.teacher_id,	pt.teacher_id, sp2.teacher_id) = afe.teacher_id -- all-time AFE sign-ups 
)


,	teacher_school_association as
(
select
at1.school_year
, at1.teacher_id
, coalesce(tsh.school_id, at1.school_id) school_id
, at1.course_name
, at1.trained_pd
, at1.teacher_started
, at1.trained_self_paced_pd
, at1.afe_signed_up 
, at1.n_students_started
, at1.n_students_committed
, at1.hours_students_started
, at1.hours_students_committed
, min(at1.registration_dt) registration_dt
from all_teachers at1
join {{ref ('int_school_years')}} sy on at1.school_year = sy.school_year
left join {{ref ('int_teacher_schools_historical')}} tsh
		on  at1.teacher_id = tsh.teacher_id 
		and sy.ended_at between tsh.started_at and tsh.ended_at
{{ dbt_utils.group_by(12) }}
)

, teacher_and_student_activity as (
select distinct
att.*
, dss.is_title_i
, dss.frl_eligible_percent 
, dss.urg_no_tr_numerator_percent pct_urg_students-- N. URG students from single-races / Total students reporting race
, dss.is_rural 
, case 
	when 	coalesce(dss.is_title_i,0) = 1 
		or	coalesce(dss.frl_eligible_percent,0) > 0.4 
		or	coalesce(dss.urg_no_tr_numerator_percent,0) > 0.3 
		or	coalesce(dss.is_rural,0) = 1
	then 'Y' 
	else 'N' 
	end afe_eligible
, upper(dss.school_name) school_name
, dt2.state state_teacher
, dss.state state_school
, coalesce(dss.state, dt2.state) state
, upper(dss.city) school_city
, dss.school_district_id
, upper(dss.school_district_name) school_district_name
, dss.zip school_zip_code
from teacher_school_association att 
left join teachers dt2
		on	att.teacher_id	=	dt2.teacher_id
left join school_stats dss   
			on att.school_id = dss.school_id
where 
(dss.state not in ('AP','GU','VI','AE','PR', 'MP')
or dss.state is null -- To include all us-based teachers, even those not associated with NCES schools
)
)

select * 
from teacher_and_student_activity