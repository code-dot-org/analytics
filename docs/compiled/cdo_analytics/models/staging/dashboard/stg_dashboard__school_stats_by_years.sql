with 
 __dbt__cte__base_dashboard__school_stats_by_years as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production"."school_stats_by_years"
),

renamed as (
    select
        school_id,
        school_year,

        lower(grades_offered_lo) as grades_offered_lo,
        lower(grades_offered_hi) as grades_offered_hi,
        
        grade_pk_offered    as is_grade_pk,
        grade_kg_offered    as is_grade_kg,
        grade_01_offered    as is_grade_01,
        grade_02_offered    as is_grade_02,
        grade_03_offered    as is_grade_03,
        grade_04_offered    as is_grade_04,
        grade_05_offered    as is_grade_05,
        grade_06_offered    as is_grade_06,
        grade_07_offered    as is_grade_07,
        grade_08_offered    as is_grade_08,
        grade_09_offered    as is_grade_09,
        grade_10_offered    as is_grade_10,
        grade_11_offered    as is_grade_11,
        grade_12_offered    as is_grade_12,
        grade_13_offered    as is_grade_13,
        
        community_type,
        virtual_status,
        title_i_status,
        
        coalesce(students_total,0)      as total_students,
        coalesce(student_am_count,0)    as count_student_am,
        coalesce(student_as_count,0)    as count_student_as,
        coalesce(student_hi_count,0)    as count_student_hi,
        coalesce(student_bl_count,0)    as count_student_bl,
        coalesce(student_wh_count,0)    as count_student_wh,
        coalesce(student_hp_count,0)    as count_student_hp,
        coalesce(student_tr_count,0)    as count_student_tr,
        coalesce(frl_eligible_total,0)  as total_frl_eligible_students,
        
        created_at,
        updated_at
    from source
)

select * 
from renamed
), school_stats_by_years as (
    select * 
    from __dbt__cte__base_dashboard__school_stats_by_years
),

survey_years as (
    select school_id,
        min(school_year) as first_survey_year,
        max(school_year) as survey_year
    from school_stats_by_years  
group by 1
),

school_stats_2019_2020 as (
    select school_id,
        total_students,
        total_frl_eligible_students
    from school_stats_by_years
    where school_year = '2019-2020'
),

school_stats_by_years_adjusted as (
    select 
        ssby.school_id,
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
        virtual_status,
        title_i_status,

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

        case when grades_offered_lo is null then null 
             when grades_offered_lo in (
                    '01',
                    '02',
                    '03',
                    '04',
                    '05',
                    'PK',
                    'KG')
                then 1
             when coalesce(
                is_grade_pk,
                is_grade_kg,
                is_grade_01,
                is_grade_02,
                is_grade_03,
                is_grade_04,
                is_grade_04,
                is_grade_05
                ) = 1 then 1
            else 0 
        end as is_stage_el,
        
        case when grades_offered_lo = 'pk'
              and grades_offered_hi = '06'
                then 0
             when grades_offered_lo = 'kg'
              and grades_offered_hi = '06'
                then 0
             when coalesce(is_grade_06,is_grade_07,is_grade_08) = 1
                then 1
             when (grades_offered_lo in ('06','07','08')
                or grades_offered_hi in ('06','07','08'))
                then 1
             when grades_offered_lo is null then null 
            else 0 
        end as is_stage_mi,
        
        case when coalesce(
                is_grade_09,
                is_grade_10,
                is_grade_11,
                is_grade_12,
                is_grade_13
                ) = 1 then 1 
             when grades_offered_hi in (
                '09',
                '10',
                '11',
                '12'
             ) then 1 
             when grades_offered_hi is null then null 
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