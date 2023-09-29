with 
source as (
      select * from {{ source('dashboard', 'school_stats_by_years') }}
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
        updated_at,
        community_type
    from source
)

select * from renamed