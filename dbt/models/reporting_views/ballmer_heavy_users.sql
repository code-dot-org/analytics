{#
model: 
auth: cory
notes: roughly based on csd_csp_completed view from analysis. This is critical for determining heavy user schools, which we use for Ballmer deliverables
changelog:
#}

with csp_csa_course_structure AS (
  SELECT
    distinct
    course_name,
    script_name,
    script_id,
    unit,
    script_name,
    stage_id,
    stage_name,
    stage_number,

    -- Extracting the unit number. 
   CASE
    when unit in ('csa1', 'csp1', 'cspunit1') then '1'
    when unit in ('csa2', 'csp2', 'cspunit2') then '2'
    WHEN unit in ('csa3', 'csp3', 'cspunit3') then '3'
    WHEN unit in ('csa4', 'csp4', 'cspunit4') then '4'
    WHEN unit in ('csa5', 'csp5', 'cspunit5') then '5'
    WHEN unit in ('csa6', 'csp6', 'cspunit6') then '6'
    WHEN unit in ('csa7', 'csp7', 'cspunit7') then '7'
    WHEN unit in ('csa8', 'csp8', 'cspunit8') then '8'
    WHEN unit in ('csa9', 'csp9', 'cspunit9') then '9'
    WHEN unit in ('csa10', 'csp10', 'cspunit10') then '10'
    else SPLIT_PART(REGEXP_SUBSTR(unit, '[0-9]+-'), '-', 1) 
    end as unit_number, 
    version_year
    -- Extracting the version year
    --NULLIF(SPLIT_PART(REGEXP_SUBSTR(script_name, '-[0-9]+$'), '-', 2), '')::INTEGER as regexp_version_year    
  FROM analytics.dim_course_structure
  WHERE course_name IN ('csp','csa')
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
	count(distinct us.stage_id) as stages -- specifying what's being counted makes it easier to understand the logic, validate, and decreases chances for error in case there's changes in the underlying table
	from analytics.dim_user_stages us
        join csp_csa_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
	    join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
	    join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
	where 
	    cs.course_name = 'csa'
        and cs.version_year in ('2022','2023','2024','2025') -- exclude pilot year 2021, course was irregularly formed
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

, int as (
select user_id, school_year, max(started_at_rank)
from csa_completed_num_stages_qualifiers
group by user_id, school_year
)

, csa_completed AS ( -- keep only users who have 5+ units with 5+ stages (the record with rank=5 from csa_num_stages is the 'completed at' time)
  SELECT
    user_id,
    'csa' as course_name,
    school_year,
    started_at as completed_at, --is this right? Worried I'm misinterpreting the syntax here
    0 as coding_qual,
    0 as noncoding_qual
  FROM csa_completed_num_stages_qualifiers
  WHERE started_at_rank=5
)

, csp_general_completed as (
    select user_id, course_name, school_year, stage_started_at::date as completed_at, 0 as coding_qual, 0 as noncoding_qual
    from
    (
        select user_id, course_name, school_year, stage_started_at, row_number() over(partition by user_id order by stage_started_at asc) script_order
        from
        (
            select us.user_id, cs.course_name, school_year, us.script_id, us.stage_id, us.stage_started_at, row_number() over(partition by us.user_id, us.script_id order by us.stage_started_at asc) stage_order
            from analytics.dim_user_stages us
            join csp_csa_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
            join analytics.int_school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
            join analytics.dim_users u on u.user_id = us.user_id and u.user_type = 'student'
            where 
                cs.course_name = 'csp'
                and cs.version_year in ('2017','2018','2019','2020','2021','2022','2023','2024','2025')
        )
        where stage_order = 5
    )
    where script_order = 4
)

, csp_coding_completed as (
    select * from
        (select user_id, course_name, school_year, max(started_at) as completed_at, sum(coding_stage_qual) coding_qual, sum(noncoding_stage_qual) noncoding_qual from 
            (select user_id, course_name, school_year, started_at,
            case when unit_number in ('3','4','5','7','9') and stages >= 5 then 1 else 0 end as coding_stage_qual,
            case when unit_number in ('1','2','6','8','9','10') and stages >= 2 then 1 else 0 end as noncoding_stage_qual
                from
                    (select us.user_id, cs.unit_number, cs.course_name, school_year, max(us.stage_started_at) as started_at, count(*) as stages
                    from analytics.dim_user_stages us
                join csp_csa_course_structure cs ON cs.script_id = us.script_id and cs.stage_id = us.stage_id
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
)

select * from csa_completed
union all
select * from csp_general_completed
union all 
select * from csp_coding_completed