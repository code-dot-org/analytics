-- model: dim_school_districts
-- scope: all dimensions currently available for school districts
-- author: AG
-- date: 2024-01-01 

with
schools as (
    select 
        school_district_id,
        -- aggregations at level of district
        count(distinct dim_schools.school_id)           as num_schools,
        sum(dim_schools.is_stage_el)                    as num_schools_stage_el,
        sum(dim_schools.is_stage_mi)                    as num_schools_stage_mi,
        sum(dim_schools.is_stage_hi)                    as num_schools_stage_hi,
        sum(dim_schools.is_rural)                       as num_schools_rural,
        sum(dim_schools.is_title_i)                     as num_schools_title_i,
        sum(dim_schools.is_high_needs)                  as num_schools_high_needs,
        sum(dim_schools.total_students)                 as num_students,
        sum(dim_schools.total_frl_eligible_students)    as num_students_frl_eligible,
        sum(total_urg_students)                         as num_students_urg
    from "dev"."dbt_jordan"."dim_schools"
    where school_district_id is not null
    group by 1
),

school_districts as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__school_districts"
    where school_district_id in (select school_district_id from schools)    
),

combined as (
    select 
        -- school district info
        school_districts.school_district_id,
        school_districts.school_district_name,
        school_districts.school_district_city,
        school_districts.school_district_state,
        school_districts.school_district_zip,
        school_districts.last_known_school_year_open,
        
        -- school info + aggs
        schools.num_schools                 as total_schools,-- are these active only?
        schools.num_schools_stage_el        as total_schools_stage_el,
        schools.num_schools_stage_mi        as total_schools_stage_mi,
        schools.num_schools_stage_hi        as total_schools_stage_hi,
        schools.num_schools_rural           as total_schools_rural,
        schools.num_schools_title_i         as total_schools_title_i,
        schools.num_schools_high_needs      as total_schools_high_needs,
        schools.num_students                as total_students,
        schools.num_students_frl_eligible   as total_students_frl_eligible,
        schools.num_students_urg            as total_students_urg,
        sum(num_students_urg / cast(num_students as float))::decimal(10,2)          as total_district_pct_students_urg,
        sum(num_students_frl_eligible / cast(num_students as float))::decimal(10,2) as total_district_pct_students_frl_eligible
    from schools
    left join school_districts
        on schools.school_district_id = school_districts.school_district_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

select * 
from combined