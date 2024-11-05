-- Model: dim_schools
-- Scope: all dimensions we have/need for schools; one row per school + school_year

with
dim_schools as (
    select *
    from {{ ref('dim_schools') }}
),

school_districts as (
    select *
    from {{ ref('stg_dashboard__school_districts') }}
),

combined as (
    select
        school_districts.school_district_id,
        school_districts.school_district_name,
        school_districts.school_district_city,
        school_districts.school_district_state,
        school_districts.school_district_zip,
        school_districts.last_known_school_year_open,

        -- school aggregations
        count(distinct dim_schools.school_id) as num_schools,
        sum(dim_schools.is_stage_el) as num_schools_stage_el,
        sum(dim_schools.is_stage_mi) as num_schools_stage_mi,
        sum(dim_schools.is_stage_hi) as num_schools_stage_hi,
        sum(dim_schools.is_rural) as num_schools_rural,
        sum(dim_schools.is_title_i) as num_schools_title_i,
        sum(dim_schools.is_high_needs) as num_schools_high_needs,
        sum(dim_schools.total_students) as num_students,
        sum(dim_schools.total_students_calculated) as num_students_calculated,
        sum(dim_schools.total_students_no_tr_calculated) as num_students_no_tr_calculated,
        sum(dim_schools.total_frl_eligible_students) as num_students_frl_eligible,
        sum(dim_schools.total_urg_students) as num_students_urg,
        sum(dim_schools.total_urg_no_tr_students) as num_students_urg_no_tr,

        -- calculations 
        round(cast(num_students_urg as float) / nullif(num_students_calculated, 0), 2) :: decimal(10, 4) as urg_with_tr_percent,
        round(cast(num_students_urg_no_tr as float) / nullif(num_students_no_tr_calculated, 0), 2) :: decimal(10, 4) as urg_no_tr_numerator_percent,
        round(cast(num_students_urg_no_tr as float) / nullif(num_students_calculated, 0), 2) :: decimal(10, 4) as urg_percent,
        round(cast(num_students_frl_eligible as float) / num_students, 2) :: decimal(10, 4) as frl_eligible_percent
    from dim_schools
    left join school_districts
        on dim_schools.school_district_id = school_districts.school_district_id 
    where dim_schools.school_district_id is not null 
    {{ dbt_utils.group_by(6) }}
)

select *
from combined