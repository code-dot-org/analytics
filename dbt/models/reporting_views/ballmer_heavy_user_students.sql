{#
model: 
auth: natalia
notes: This counts heavy user students. This is roughly based on csd_csp_completed view from analysis. This is critical for determining heavy user schools, which we use for Ballmer deliverables
changelog:
#}
with
course_users as (
        select
            us.student_id as user_id,
            us.course_name,
            us.school_year,
            us.section_id,
            us.section_teacher_id,
            us.school_id,
            cs.unit,
            (case when regexp_substr (cs.unit, '(\\d{1}|\\d{2})') <> '' then regexp_substr (cs.unit, '(\\d{1}|\\d{2})') else null end)::int unit_number,
            max (case when regexp_substr (cs.version_year, '(\\d{4})') <> '' then regexp_substr (cs.version_year, '(\\d{4})') else null end)::int version_year, -- if students have activity in different versions, we'll use the most recent one for that unit
            count(distinct us.lesson_id) as n_stages  -- specifying what's being counted makes it easier to understand the logic, validate, and decreases chances for error in case there's changes in the underlying table
        from {{ ref('dim_student_script_level_activity') }} us
        join
            {{ ref('dim_course_structure') }} cs
            on us.script_id = cs.script_id
            and us.level_id = cs.level_id
        where
            us.course_name in ('csa', 'csp')
            and cs.unit not in (
                'csa-consumer-review-lab',
                'csa-data-lab',
                'csa-labs',
                'csa-magpie-lab',
                'csa-postap-se-and-computer-vision',
                'csa-preview',
                'csa-software-engineering',
                'csp-create',
                'csp-explore',
                'csp3-research-mxghyt'
            ) -- exclude standalone units that are not part of the core AP content
            and us.user_type = 'student'
            and us.level_type != 'StandaloneVideo' -- Excluding standalone video levels as these are not activity levels
            and cs.version_year not in ('unversioned')
        group by 1, 2, 3, 4, 5, 6, 7, 8
    )

--    
    , 
    course_coding_completed_qualifiers as -- defines qualifying criteria per user/unit/course/script version
	(    
            select
            user_id,
            course_name,
            version_year,
            unit,
            unit_number,
            school_year,
            n_stages,
                count (distinct case
                    when
                        course_name = 'csa'	
                        and n_stages >= 5
                    then unit
                    when
                    	course_name = 'csp'	
                        and version_year < 2020 
                        and  n_stages >= 5
                    then unit
                    else null
                end) as general_qual_by_unit, -- assigns 1 if user has 5 or more lessons (stages) per unit, null otherwise. This is used for CSA and for CSP scripts from versions before 2020  
                count (distinct case
	                when
                        course_name = 'csp'	
                        and version_year between 2020 and 2022 
                        and unit_number  in ('3', '4', '5', '7', '9') -- programming units for CSP versions 2020, 2021 and 2022
                        and n_stages >= 5
                    then unit
                    when
                        course_name = 'csp'	
                        and version_year > 2022 
                        and unit_number in ('3', '4', '5', '6', '7', '9') -- programming units for CSP versions 2023 and after
                        and n_stages >= 5
                    then unit
                    else null
                end) as coding_stage_qual_by_unit, -- assigns 1 if user has 5 or more lessons (stages) per unit for CSP coding units 
                count (distinct case
                    when
                    	course_name = 'csp'
                    	and version_year between 2020 and 2022 
                        and unit_number  in ('1', '2', '6', '8', '9', '10') --non programming units for CSP versions 2020, 2021 and 2022
                        and n_stages >= 2
                    then unit
	                when
                    	course_name = 'csp'
                        and version_year > 2022 
                        and unit_number in ('1', '2', '5', '8', '10') --non programming units for CSP versions 2023 and after
                        and n_stages >= 2
                    then unit
                    else null
                end ) as noncoding_stage_qual_by_unit -- assigns 1 if user has 5 or more lessons (stages) per unit for CSP non-coding units 
        from course_users
        where unit_number is not null 
        group by
        	user_id,
            course_name,
            school_year,
            unit,
            unit_number,
            version_year,
            n_stages
    )
--    
    ,
       course_completed_qualifiers as (  -- keeps only users/unit records that meet qualifying criteria per unit as defined in course_coding_completed_qualifiers
        select distinct
            cu.user_id,
            cu.course_name,
            cu.version_year,
            cu.school_year,
            cu.section_id,
            cu.section_teacher_id,
            cu.school_id,
            sum(cq.general_qual_by_unit) over (
                partition by cu.user_id, cu.school_year, cu.course_name, cu.section_id) general_unit_qual, -- number of total units meeting qualifying criteria
			sum(cq.coding_stage_qual_by_unit) over (
                partition by cu.user_id, cu.school_year, cu.course_name, cu.section_id) coding_unit_qual, -- number of coding units meeting qualifying criteria
			sum(cq.noncoding_stage_qual_by_unit) over (
                partition by cu.user_id, cu.school_year, cu.course_name, cu.section_id) noncoding_unit_qual -- number of noncoding units meeting qualifying criteria
        from course_users cu
        left join course_coding_completed_qualifiers cq on 
        cu.user_id = cq.user_id
		and cu.course_name = cq.course_name
		and cu.unit = cq.unit
		and cu.unit_number = cq.unit_number
		and cu.school_year = cq.school_year
		)

--
    ,
    course_completed as -- defines qualifying criteria per user/course/script version and keeps only user/course/school_year records that meet it
    ( 
        select distinct
            user_id,
            course_name,
            school_year,
            section_id, 
            section_teacher_id,
            school_id,
            coalesce(general_unit_qual, 0) as general_qual,
            coalesce(coding_unit_qual, 0) as coding_qual,
            coalesce(noncoding_unit_qual, 0) as noncoding_qual
        from course_completed_qualifiers
        where 
        ( 	course_name = 'csa' and general_unit_qual >= 5) 							-- CSA - general: users with at least 5 units meeting qualifying criteria for CSA
        or (course_name = 'csp' and version_year < 2020 and general_unit_qual >= 4)		-- CSP - general for CSP scripts from versions before 2020: users with at least 4 units meeting qualifying criteria  
        or (course_name = 'csp' and version_year >= 2020 and coding_unit_qual >= 3)		-- CSP - coding for CSP scripts from versions 2020 and after: users with at least 3 coding units meeting qualifying criteria
        or (course_name = 'csp' and version_year >= 2020 and noncoding_unit_qual >= 3)	-- CSP - non-coding for CSP scripts from versions 2020 and after: users with at least 3 non-coding units meeting qualifying criteria
    )

select
school_year, course_name, count(distinct user_id)
from course_completed
group by school_year, course_name
order by course_name desc, school_year desc

select
*
from course_completed
