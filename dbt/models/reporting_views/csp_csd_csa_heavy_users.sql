{#
model: 
auth: cory
notes: ports the csd_csp_complete view from analysis to work on Hydrone. This is critical for determining heavy user schools, which we use for Ballmer deliverables
changelog:
#}

with csp_csd_course_structure AS (
  -- to do: this does not yet account for non-regularly named scripts (e.g. aiml-2020, cspunit5, etc.)
  SELECT
    distinct
    course_name,
    script_name,
    script_id,
    LEFT(script_name, 4) unit_name, --e.g. csd3,csd4,csp7,csp10, etc.
    stage_id,
    stage_name,
    stage_number,

    -- Extracting the unit number. 
    SPLIT_PART(REGEXP_SUBSTR(script_name, '[0-9]+-'), '-', 1) as unit_number, --used for csp definitions
    -- Extracting the version year
    NULLIF(SPLIT_PART(REGEXP_SUBSTR(script_name, '-[0-9]+$'), '-', 2), '')::INTEGER as version_year
    
    --, course_name versioned_course_name --mka
    
  FROM analytics.dim_course_structure
  WHERE course_name IN ('csp','csd','csa')
)

, first_semester as
-- date when student finished first semester of CSD
(
select user_id, course_name, school_year, stage_started_at::date as completed_at
from
(
  select user_id, course_name, school_year, stage_started_at, row_number() over(partition by user_id order by stage_started_at asc) script_order
  from
  (
    select us.user_id, cs.course_name, school_year, cs.script_name, us.stage_id, us.stage_started_at, row_number() over(partition by us.user_id, cs.script_name order by us.stage_started_at asc) stage_order
    from analytics.dim_user_stages us
    join csp_csd_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
    where 
      cs.course_name = 'csd'
      and unit_name IN ('csd2','csd3')
  )
  where stage_order = 5 
)
where script_order = 2
)

, second_semester_two_unit as
-- date when student finished second semester of CSD, 
-- using 2 unit definition (2 lessons in each of units 4 and 5)
(
select user_id, course_name, school_year, stage_started_at::date as completed_at
from
(
  select user_id, course_name, school_year, stage_started_at, row_number() over(partition by user_id order by stage_started_at asc) script_order
  from
  (
    select us.user_id, cs.course_name, school_year, cs.script_name, us.stage_id, us.stage_started_at, row_number() over(partition by us.user_id, cs.script_name order by us.stage_started_at asc) stage_order
    from analytics.dim_user_stages us
    join csp_csd_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
    where 
      cs.course_name = 'csd'
      and unit_name IN ('csd4','csd5')
  )
  where stage_order = 2
)
where script_order = 2
),

second_semester_one_unit as
-- date when student finished second semester of CSD, 
-- using 1 unit definition (5 lessons in unit 6)
(
select user_id, course_name, school_year, stage_started_at::date as completed_at
from
(
  select user_id, course_name, school_year, stage_started_at, row_number() over(partition by user_id order by stage_started_at asc) script_order
  from
  (
    select us.user_id, cs.course_name, school_year, cs.script_name, us.stage_id, us.stage_started_at, row_number() over(partition by us.user_id, cs.script_name order by us.stage_started_at asc) stage_order
    from analytics.dim_user_stages us
    join csp_csd_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
    where 
      cs.course_name = 'csd'
      and unit_name IN ('csd6')
  )
  where stage_order = 5
)
)

------ CTEs for csa_completed -----
-- Stacked solution:
-- 1. Find all csa_users of units 1-8
-- 2. keep only users/units that have 5+ lessons visited
-- 3. keep only users that have 5+ units with 5+lessons
, csa_users AS ( -- user|unit|school_year|stagecount --all students by # stages per unit per school year
  select 
		    us.user_id, 
		    cs.course_name, 
		    school_year,
		    cs.unit_number, 
		    max(us.stage_started_at) as started_at,  
	--	    count(*) as stages
	            count(distinct us.stage_id) as stages -- specifying what's being counted makes it easier to understand the logic, validate, and decreases chances for error in case there's changes in the underlying table
	    from analytics.dim_user_stages us
      join csp_csd_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
	    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
	    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
	    where 
	       cs.course_name = 'csa'
         and cs.version_year >= 2022 -- exclude pilot year 2021, course was irregularly formed
	    group by 1,2,3,4
	    having stages >= 5 -- keeps only students with at least 5 stages started per unit 
	    ORDER BY user_id, unit_number, school_year
)

, csa_completed_num_stages_qualifiers AS ( -- keep only user/unit records that have 5+ stages visited (this keeps about 67-80% of students per unit)
  SELECT
      user_id,
      unit_number,
      school_year,
      started_at,
      stages,
      ROW_NUMBER() OVER (PARTITION BY user_id,school_year ORDER BY started_at ASC) as started_at_rank --this gives a ranking to each unit for each user in order they started it

    FROM
      csa_users 
    WHERE
      unit_number IS NOT NULL and unit_number BETWEEN 1 AND 8
)

, csa_completed AS ( -- keep only users who have 5+ units with 5+ stages (the record with rank=5 from csa_num_stages is the 'completed at' time)
  SELECT
    user_id,
    school_year,
    started_at completed_at
  FROM csa_completed_num_stages_qualifiers
  WHERE started_at_rank=5
)

----------Build table unioning csd, csp and csa defs --------------------
select 
  fs.user_id, 
  case 
    when sst.user_id is not null or sso.user_id is not null then 'csd'
    else 'csd_half'
  end as course_name,
  fs.school_year,
  case
    -- logic first looks for the first second semester completion date (least()), 
    -- then compares to first semester completion date (greatest())
    when sst.user_id is not null or sso.user_id is not null then greatest(fs.completed_at, least(sst.completed_at, sso.completed_at))
    else fs.completed_at
  end as completed_at, 0 as coding_qual, 0 as noncoding_qual
from first_semester fs
left join second_semester_two_unit sst on sst.user_id = fs.user_id and sst.school_year = fs.school_year
left join second_semester_one_unit sso on sso.user_id = fs.user_id and sso.school_year = fs.school_year

union all

select user_id, course_name, school_year, stage_started_at::date as completed_at, 0 as coding_qual, 0 as noncoding_qual
from
(
  select user_id, course_name, school_year, stage_started_at, row_number() over(partition by user_id order by stage_started_at asc) script_order
  from
  (
    select us.user_id, cs.course_name, school_year, us.script_id, us.stage_id, us.stage_started_at, row_number() over(partition by us.user_id, us.script_id order by us.stage_started_at asc) stage_order
    from analytics.dim_user_stages us
    join csp_csd_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
    where 
	    cs.course_name = 'csp'
	    and cs.version_year IN ('2017','2018','2019')
  )
  where stage_order = 5
)
where script_order = 4

union all

select * from
(select user_id, course_name, school_year, max(started_at) as completed_at, sum(coding_stage_qual) coding_qual, sum(noncoding_stage_qual) noncoding_qual from 
	(select user_id, course_name, school_year, started_at,
  case when unit_number in ('3','4','5','7','9') and stages >= 5 then 1 else 0 end as coding_stage_qual,
  case when unit_number in ('1','2','6','8','9','10') and stages >= 2 then 1 else 0 end as noncoding_stage_qual
	from
		(select us.user_id, cs.unit_number, cs.course_name, school_year, max(us.stage_started_at) as started_at, count(*) as stages
	    from analytics.dim_user_stages us
      join csp_csd_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
	    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
	    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
	    where 
	    cs.course_name = 'csp'
	    and cs.version_year >= 2020
	    
	    group by 1,2,3,4) stg
	 ) unt
group by 1,2,3
) prod
where (coding_qual >= 3) or (noncoding_qual >= 3)

union all

select

  user_id,
  'csa' as course_name,
  school_year,
  completed_at,
  0 as coding_qual,
  0 as noncoding_qual

FROM csa_completed