with
school_stats_by_years as (
    select
        *,
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

        total_students,
        student_am_count,
        student_as_count,
        student_hi_count,
        student_bl_count,
        student_wh_count,
        student_hp_count,
        student_tr_count,

        sum(student_am_count+
            student_as_count+
            student_hi_count+
            student_bl_count+
            student_wh_count+
            student_hp_count+
            student_tr_count)::int as total_students_calculated, 

        sum(student_am_count+
            student_as_count+
            student_hi_count+
            student_bl_count+
            student_wh_count+
            student_hp_count)::int as total_students_no_tr_calculated,

        sum(student_am+
            student_hi+
            student_bl+
            student_hp)::int as total_students_urg_calculated, 


        min(school_year) as first_survey_year,
        max(school_year) as survey_year

    from {{ ref('base_dashboard__school_stats_by_years') }}
    {{ dbt_utils.group_by(37) }}
),

final as (
    select *,
        case when total_students = student_total_calculated
                then total_students_urg_calculated / total_students::float 
        end                                                                     as pct_urg_students_calculated,
        
        case when total_students_calculated / total_students::float >= 0.7
                then total_students_urg_calculated/ total_students_calculated
        end                                                                     as pct_urg_students_calculated_true,

        case when total_students_no_tr_calculated > 0 
                then total_students_urg_calculated / total_students_calculated::float 
        end                                                                     as pct_urg_students_no_tr

        case when total_students < total_frl_eligible 
                then total_frl_eligible / total_students::float 
        end                                                                     as pct_frl_eligible

        case when coalesce(total_frl_eligible,0) > 0.5 then 1 else 0 end as is_high_needs
    from school_stats_by_years
)

select *
from final
