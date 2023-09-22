with
school_stats_by_years as (
    select
        -- from base
        school_id,
        school_year,
        grades_offered_lo,
        grades_offered_hi,

        case
            when grades_offered_lo is null then null
            when
                grades_offered_lo in (
                    '01',
                    '02',
                    '03',
                    '04',
                    '05',
                    'PK',
                    'KG'
                )
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

        case
            when grades_offered_lo is null then null
            when
                lower(grades_offered_lo) = 'pk'
                and grades_offered_hi = '06'
                then 0
            when
                lower(grades_offered_lo) = 'kg'
                and grades_offered_hi = '06'
                then 0
            when coalesce(is_grade_06, is_grade_07, is_grade_08) = 1
                then 1
            when (
                grades_offered_lo in ('06', '07', '08')
                or grades_offered_hi in ('06', '07', '08')
            )
                then 1
            else 0
        end as is_stage_mi,

        case
            when grades_offered_lo is null then null
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
        end as is_stage_hi,

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

        case
            when title_i_status in (1, 2, 3, 4, 5)
                then 1
            when title_i_status = 6
                then 0
        end as is_title_i,

        case
            when
                community_type in (
                    'rural_fringe',
                    'rural_distant',
                    'rural_remote',
                    'town_remote',
                    'town_distant')
                then 1
            when community_type is not null
                then 0
        end as is_rural,

        student_am_count,
        student_as_count,
        student_hi_count,
        student_bl_count,
        student_wh_count,
        student_hp_count,
        student_tr_count,
        total_frl_eligible_students,
        total_students
    from {{ ref('base_dashboard__school_stats_by_years') }}
)

select * 
from school_stats_by_years