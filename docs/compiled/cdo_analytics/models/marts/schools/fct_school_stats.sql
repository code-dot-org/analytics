with 

schools as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__schools"
),

school_stats_by_years as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__school_stats_by_years"
),

school_districts as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__school_districts"
),

school_stats_19_20 as (
    select *
    from school_stats_by_years
    where school_year = '2019-2020'
),

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
        case 
            when survey_years.survey_year = '2020-2021' then coalesce(ssby2.total_frl_eligible, ssby.total_frl_eligible)   ---- coalesce statement added to next three fields to account for covid FRL effects
            else ssby.total_frl_eligible
        end as frl_eligible,
        case 
            when survey_years.survey_year = '2020-2021' 
            then coalesce(
                (case  
                    when ssby2.total_frl_eligible is null or ssby2.total_students is null or ssby2.total_frl_eligible > ssby2.total_students then null
                    else ssby2.total_frl_eligible / ssby2.total_students::float 
                end
                ),
                (case
                    when ssby.total_frl_eligible is null or ssby.total_students is null or ssby.total_frl_eligible > ssby.total_students then null
                    else ssby.total_frl_eligible / ssby.total_students::float 
                end
                )
            )
            else ssby.frl_eligible_percent
        end as frl_eligible_percent,    
        case 
            when survey_years.survey_year = '2020-2021' 
            then coalesce(
                (case 
                    when ssby2.total_frl_eligible is null or ssby2.total_students is null then null 
                    when (ssby2.total_frl_eligible / ssby2.total_students::float) > 0.5 then 1 
                    else 0 
                end
                ),
                (case 
                    when ssby.total_frl_eligible is null or ssby.total_students is null then null
                    when (ssby.total_frl_eligible / ssby.total_students::float) > 0.5 then 1 else 0 
                end
                )
            )
            else ssby.is_high_needs 
        end as is_high_needs,
        
        -- end altered code
        ssby.is_title_i,
        ssby.community_type                                         as community_type,
        ssby.is_rural                                               as is_rural
    from schools
    left join school_districts
    on schools.school_district_id = school_districts.school_district_id
    left join 
        (select 
            max(school_year) as survey_year,
            min(school_year) as first_survey_year,
            school_id
        from school_stats_by_years
        group by school_id) survey_years
    on survey_years.school_id = schools.school_id
    left join school_stats_by_years ssby
    on ssby.school_id = schools.school_id
    and ssby.school_year = survey_years.survey_year
    left join school_stats_19_20  ssby2  -----  this join is new to facilitate the FRL stop gap logic.
    on ssby2.school_id = schools.school_id
) 

select * 
from combined