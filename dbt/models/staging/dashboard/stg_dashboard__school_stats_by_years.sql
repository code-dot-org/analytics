with
school_stats_by_years as (
    select *
    from {{ ref('base_dashboard__school_stats_by_years') }}
),

survey_years as (
    select
        school_id,
        min(school_year) as first_survey_year,
        max(school_year) as survey_year
    from school_stats_by_years
    {{ dbt_utils.group_by(1) }}
),

school_stats_2019_2020 as (
    select
        school_id,
        total_students,
        total_frl_eligible_students
    from school_stats_by_years
    where school_year = '2019-2020'
),

school_stats_by_years_adjusted as (
    select
    {{ pad_school_id('ssby.school_id') }}  as school_id,
        school_year,
        survey_year,
        first_survey_year,
        grades_offered_lo,
        grades_offered_hi,
        is_grade_pk,
        is_grade_kg,
        is_grade_01,
        is_grade_02,
        is_grade_03,
        is_grade_04,
        is_grade_05,
        is_grade_06,
        is_grade_07,
        is_grade_08,
        is_grade_09,
        is_grade_10,
        is_grade_11,
        is_grade_12,
        is_grade_13,
        lower(virtual_status) as virtual_status,
        lower(title_i_status) as title_i_status,

        -- adjust for frl stop-gap logic
        nullif(
            case when survey_years.survey_year = '2020-2021'
                and ssby.total_students is null 
                    then ssby2.total_students
                else ssby.total_students
        end, 0) as total_students,

        count_student_am,
        count_student_as,
        count_student_hi,
        count_student_bl,
        count_student_wh,
        count_student_hp,
        count_student_tr,

        -- adjustment for FRL logic
        case when survey_year = '2020-2021'
             and ssby.total_frl_eligible_students is null 
                then ssby2.total_frl_eligible_students
            else ssby.total_frl_eligible_students 
        end as total_frl_eligible_students,

        created_at,
        updated_at,
        community_type,

        case when title_i_status in (1,2,3,4,5) 
                then 1
            when title_i_status = 6 
                then 0
        end as is_title_i,

        case when community_type in (
                    'rural_fringe',
                    'rural_distant',
                    'rural_remote',
                    'town_remote',
                    'town_distant')
                then 1 
            when community_type is not null 
                then 0
        end as is_rural,

        case 
            when grades_offered_lo in ('01', '02', '03', '04', '05', 'pk', 'kg') then 1
            when 
                is_grade_pk = 1 
                OR is_grade_kg = 1 
                OR is_grade_01 = 1 
                OR is_grade_02 = 1 
                OR is_grade_03 = 1 
                OR is_grade_04 = 1 
                OR is_grade_05 = 1 
            then 1
            when grades_offered_lo is null then null -- I don't this is possible unless everything else is already null
            else 0 
        end as is_stage_el,

        
        case 
            when grades_offered_lo IN ('pk','kg')  -- exclude K-6 and pre-K-6 schools from being classified as middle schools
              and grades_offered_hi = '06'
                then 0

            when                                    -- any school offering grade 6, 7 or 8 is middle school
                is_grade_06=1 
                OR is_grade_07=1 
                OR is_grade_08 = 1
                then 1

            when 
                (grades_offered_lo in ('06','07','08') --if no individual grades are marked, look at lo/hi. if lo/hi in grade 6,7,8 it's middle school
                or grades_offered_hi in ('06','07','08'))
                then 1

            when 
                grades_offered_lo is null 
                then null 
            else 0 
        end as is_stage_mi,
        
        case                                            --if any individual grade 9-13 is marked as 1, then it's HS
            when 
                is_grade_09 = 1
                OR is_grade_10 = 1
                OR is_grade_11 = 1
                OR is_grade_12 = 1
                OR is_grade_13 = 1
                then 1 

             when grades_offered_hi in ( '09','10','11','12') 
                then 1 
             when 
                grades_offered_hi is null 
                then null 
            else 0 
        end as is_stage_hi

    from school_stats_by_years as ssby
    left join school_stats_2019_2020 as ssby2
        on ssby.school_id = ssby2.school_id
    left join survey_years
        on ssby.school_id = survey_years.school_id
)

select *
from school_stats_by_years_adjusted
