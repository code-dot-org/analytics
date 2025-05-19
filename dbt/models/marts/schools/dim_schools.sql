/*
Model: dim_schools
Scope: all dimensions we have/need for schools; one row per school, most recent data. 

Note on schools with NULL last_survey_year - there are 5500 schools wth no last_survey_year. The vast majority were from manual uploads in 2017-2018. These schools have no valid NCES match. However, teachers are still selecting them from the dropdown list. Over 150 of those schools were active in 2024-25. Until those schools are removed from the dropdown list and teachers are prompted to reaffiliate, we will keep them in the list of schools for now. 

Edit log
- CK - May 2025 - Added county ID and name, count by gender, open status, and title I detail
*/



with
schools as (
    select *
    from {{ ref('stg_dashboard__schools') }}
)

, school_districts as (
    select *
    from {{ ref('stg_dashboard__school_districts') }}
)

, school_stats_by_years as (
    select *, 
        row_number() 
            over(
                partition by school_id 
                order by school_year desc) as row_num

    from {{ ref('dim_school_stats_by_years') }}
)

, combined as (
    select
        schools.school_id,
        schools.city,
        schools.state,
        schools.zip,

        school_stats_by_years.school_year   as last_survey_year,
        school_stats_by_years.is_stage_el,
        school_stats_by_years.is_stage_mi,
        school_stats_by_years.is_stage_hi,
        
        (
            (case when school_stats_by_years.is_stage_el = 1 then 'el' else '__' end ) ||
            (case when school_stats_by_years.is_stage_mi = 1 then 'mi' else '__' end ) ||
            (case when school_stats_by_years.is_stage_hi = 1 then 'hi' else '__' end ) 
        ) as school_level_simple,

        school_stats_by_years.is_rural,
        school_stats_by_years.title_i_status,
        school_stats_by_years.is_title_i,
        school_stats_by_years.community_type,

        schools.school_category,
        schools.school_name,
        schools.school_type,
        case when total_frl_eligible_students / total_students::float > 0.5
            then 1 else 0 
        end as is_high_needs,

        schools.last_known_school_year_open,

        --school_districts
        school_districts.school_district_id,
        school_districts.school_district_name,

        --county
        schools.county_id,
        schools.county_name,

        -- nces school metrics
        school_stats_by_years.total_students,
        school_stats_by_years.count_student_female,
        school_stats_by_years.count_student_male,
        school_stats_by_years.count_student_am,
        school_stats_by_years.count_student_as,
        school_stats_by_years.count_student_hi,
        school_stats_by_years.count_student_bl,
        school_stats_by_years.count_student_wh,
        school_stats_by_years.count_student_hp,
        school_stats_by_years.count_student_tr,
        school_stats_by_years.total_frl_eligible_students,
        school_stats_by_years.total_urg_students,
        school_stats_by_years.total_urg_no_tr_students,
        school_stats_by_years.total_students_calculated,
        school_stats_by_years.total_students_no_tr_calculated,
        school_stats_by_years.urg_percent,
        school_stats_by_years.urg_with_tr_percent,
        school_stats_by_years.urg_no_tr_numerator_percent,
        school_stats_by_years.frl_eligible_percent,

        
        --open status
        school_stats_by_years.open_status,
        school_stats_by_years.is_school_open,
        
        -- dates 
        min(schools.created_at) as school_created_at,
        max(schools.updated_at) as school_last_updated_at


    from schools
    left join school_stats_by_years
        on schools.school_id = school_stats_by_years.school_id
        and school_stats_by_years.row_num = 1
    
    left join school_districts
        on schools.school_district_id = school_districts.school_district_id
    
    {{ dbt_utils.group_by(43) }}
)

select *
from combined