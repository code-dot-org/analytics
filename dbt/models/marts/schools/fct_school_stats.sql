with 
school_stats_by_years as (
    select *,  
    from {{ ref('stg_dashboard__school_stats_by_years') }}
),

school_stats_adjusted as (
    select *,

        lag(total_students,1) as prev_total_students,
        
        (student_am_count + 
        student_as_count + 
        student_hi_count + 
        student_bl_count + 
        student_wh_count + 
        student_hp_count + 
        student_tr_count) as total_students_calculated,
        
        case when total_students = total_students_calculated
                then total_students_urg_calculated / total_students::float 
        end as pct_urg_students_calculated,
        
        case when total_students_calculated / total_students::float >= 0.7
                then total_students_urg_calculated/ total_students_calculated
        end as pct_urg_students_calculated_true,

        case when total_students_no_tr_calculated > 0 
                then total_students_no_tr_calculated / total_students_calculated::float 
        end as pct_urg_students_no_tr,

        case when total_students < total_frl_eligible 
                then total_frl_eligible / total_students::float 
        end as pct_frl_eligible

        case when total_frl_eligible > 0.5 then 1 else 0 end as is_high_needs,

    from school_stats_by_years
),

-- adjusting this school year for Covid FRL effects
school_stats_19_20 as (
    select school_id,
        case when total_students is null then lag(total_students
        coalesce(total_students, total_frl_eligible_students
    from school_stats_adjusted
    where school_year = '2020-2021'
),

select * 
from combined
/*
combined as (
    select 
        schools.school_id                                                   as school_id,
        schools.school_name                                                 as school_name,
        schools.city                                                        as city,
        schools.zip                                                         as zip,
        schools.state                                                       as state,
        schools.latitude                                                    as latitude,
        schools.longitude                                                   as longitude,
        schools.school_type                                                 as school_type,
        school_districts.school_district_id                                 as school_district_id,
        school_districts.school_district_name                               as school_district_name,
        survey_years.survey_year                                            as survey_year,
        survey_years.first_survey_year                                      as first_survey_year,
        ssby.grades_offered_lo                                              as grades_lo,
        ssby.grades_offered_hi                                              as grades_hi,
        ssby.is_stage_el                                                    as is_stage_el,
        ssby.is_stage_mi                                                    as is_stage_mi,
        ssby.is_stage_hi                                                    as is_stage_hi,
        ssby.total_students                                                 as total_students,
        ssby.count_student_am                                               as count_student_am,
        ssby.count_student_as                                               as count_student_as,
        ssby.count_student_hi                                               as count_student_hi,
        ssby.count_student_bl                                               as count_student_bl,
        ssby.count_student_wh                                               as count_student_wh,
        ssby.count_student_hp                                               as count_student_hp,
        ssby.count_student_tr                                               as count_student_tr,
        ssby.students_summed,
        ssby.urg_percent,
        ssby.urg_percent_true,  ----------------  adds all schools where sum <> total reported students, this allows us to extrapolate percentages at the school
        ssby.urg_percent_no_tr,
        
        /* this next section is where we made the edits to support the new FRL stop-gap logic*/
