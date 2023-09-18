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