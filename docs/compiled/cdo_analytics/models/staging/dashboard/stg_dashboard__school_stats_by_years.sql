with 

 __dbt__cte__base_dashboard__school_stats_by_years as (
with 
source as (
      select * from "dashboard"."dashboard_production"."school_stats_by_years"
),

renamed as (
    select
        school_id,
        school_year,
        grades_offered_lo,
        grades_offered_hi,
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
        virtual_status,
        students_total      as total_students,
        student_am_count    as count_student_am,
        student_as_count    as count_student_as,
        student_hi_count    as count_student_hi,
        student_bl_count    as count_student_bl,
        student_wh_count    as count_student_wh,
        student_hp_count    as count_student_hp,
        student_tr_count    as count_student_tr,
        title_i_status,
        frl_eligible_total  as total_frl_eligible,
        created_at,
        updated_at,
        community_type
    from source
)

select * from renamed
), school_stats_by_years as (
    select *,
        
        case when title_i_status in (1,2,3,4,5) 
                then 1
            when title_i_status = 6 
                then 0
        end                                                                             as is_title_i,

        case when community_type in (
                    'rural_fringe',
                    'rural_distant',
                    'rural_remote',
                    'town_remote',
                    'town_distant')
                then 1 
            when community_type is not null 
                then 0
        end                                                                             as is_rural,

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
        end                                                                            as is_stage_el,
        
        case when grades_offered_lo is null then null 
             when lower(grades_offered_lo) = 'pk'
                and grades_offered_hi = '06'
                    then 0
             when lower(grades_offered_lo) = 'kg'
                and grades_offered_hi = '06'
                    then 0
             when coalesce(is_grade_06,is_grade_07,is_grade_08) = 1
                    then 1
             when (grades_offered_lo in ('06','07','08')
                or grades_offered_hi in ('06','07','08'))
                    then 1
            else 0 
        end                                                                            as is_stage_mi,
        
        case when grades_offered_lo is null then null 
             when coalesce(
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
            else 0 
        end                                                                             as is_stage_hi,

        sum(count_student_am 
            + count_student_as
            + count_student_hi
            + count_student_bl
            + count_student_wh
            + count_student_hp)                                                         as total_urg_no_tr_students,

        sum(count_student_am
            + count_student_as
            + count_student_hi
            + count_student_bl
            + count_student_wh
            + count_student_hp
            + count_student_tr)                                                         as total_urg_students,

        coalesce(count_student_am,0)  
            + coalesce(count_student_as,0)  
            + coalesce(count_student_hi,0)  
            + coalesce(count_student_bl,0)  
            + coalesce(count_student_wh,0)  
            + coalesce(count_student_hp,0)  
            + coalesce(count_student_tr,0)                                              as students_summed,  ---------------  sh to allow for manipulation later
        
        case 
            when total_students = coalesce(count_student_am,0)  
                                    + coalesce(count_student_as,0)  
                                    + coalesce(count_student_hi,0)  
                                    + coalesce(count_student_bl,0)  
                                    + coalesce(count_student_wh,0)  
                                    + coalesce(count_student_hp,0)  
                                    + coalesce(count_student_tr,0) 
            then (coalesce(count_student_am,0)  
                + coalesce(count_student_hi,0)  
                + coalesce(count_student_bl,0)  
                + coalesce(count_student_hp,0)) 
                / total_students::float 
        end                                                                             as urg_percent,
        case 
            when .7 <= 
                (coalesce(count_student_am,0)  
                + coalesce(count_student_as,0)  
                + coalesce(count_student_hi,0)  
                + coalesce(count_student_bl,0)  
                + coalesce(count_student_wh,0)  
                + coalesce(count_student_hp,0)  
                + coalesce(count_student_tr ,0))::float
                /total_students::float
            then 
                (coalesce(count_student_am,0)  
                + coalesce(count_student_hi,0)  
                + coalesce(count_student_bl,0)  
                + coalesce(count_student_hp,0)) / 
                (coalesce(count_student_am,0)  
                + coalesce(count_student_as,0)  
                + coalesce(count_student_hi,0)  
                + coalesce(count_student_bl,0)  
                + coalesce(count_student_wh,0)  
                + coalesce(count_student_hp,0)  
                + coalesce(count_student_tr ,0))::float 
        end                                                                              as urg_percent_true,
        case 
            when 0 < coalesce(count_student_am,0)  
                    + coalesce(count_student_as,0)  
                    + coalesce(count_student_hi,0)  
                    + coalesce(count_student_bl,0)  
                    + coalesce(count_student_wh,0)  
                    + coalesce(count_student_hp,0)
            then 
                (coalesce(count_student_am,0)  
                + coalesce(count_student_hi,0)  
                + coalesce(count_student_bl,0)  
                + coalesce(count_student_hp,0)) / 
                (coalesce(count_student_am,0)  
                + coalesce(count_student_as,0)  
                + coalesce(count_student_hi,0)  
                + coalesce(count_student_bl,0)  
                + coalesce(count_student_wh,0)  
                + coalesce(count_student_hp,0))::float 
        end                                                                                 as urg_percent_no_tr,
        
        case 
            when total_frl_eligible is null 
                or total_students is null 
                or total_frl_eligible > total_students 
            then null
            else total_frl_eligible / total_students::float 
        end                                                                                 as frl_eligible_percent,    
        case 
            when total_frl_eligible is null 
                or total_students is null 
            then null 
            when (total_frl_eligible / total_students::float) > 0.5
            then 1 
            else 0 
        end                                                                                 as is_high_needs,

        min(school_year) as first_survey_year,
        max(school_year) as survey_year

    from __dbt__cte__base_dashboard__school_stats_by_years
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
)

select * 
from school_stats_by_years