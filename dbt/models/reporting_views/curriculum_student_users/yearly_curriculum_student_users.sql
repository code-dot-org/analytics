/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 curriculum student user goals
Does all the calculations to generate the school-year level reporting metrics

Edits:
3/25: CK edits to include global
*/

with curriculum_counts as (
    select *
    from {{ref('dim_curriculum_student_users')}}
    where qualifying_date >= '2020-07-01' --replace with dynamic reference
)

--duca used here because the total includes 1+ days of HS and that's not in dim_curriculum_student_users
, all_counts as (
    select 
        school_year,
        us_intl,
        country,
        count(distinct user_id) as n_students
    from 
        {{ref('dim_user_course_activity')}}
    where 
        user_type = 'student' and
        content_area like '%curriculum%' and
        first_activity_at >= '2020-07-01' and 
        country is not null
    group by 1,2,3
)

, grade_band_metrics as(
    select 
        curriculum_counts.school_year
        , curriculum_counts.us_intl
        , country
        , count (distinct case when grade_band = 'HS' then curriculum_counts.student_id else null end ) n_students_HS
        , count (distinct case when grade_band = 'MS' then curriculum_counts.student_id else null end ) n_students_MS
        , count (distinct case when grade_band = 'ES' then curriculum_counts.student_id else null end ) n_students_ES 
    from curriculum_counts
    group by 1,2,3
)

, us_metric_prep as (
    select 
        school_year
        , count (distinct case when grade_band = 'HS' then student_id else null end ) n_students_hs
        , count (distinct case when grade_band = 'MS' then student_id else null end ) n_students_ms
        , count (distinct case when grade_band = 'HS' and gender_group = 'f' then student_id else null end ) n_students_hs_f
        , count (distinct case when grade_band = 'HS' and gender_group in ('m','nb') then student_id else null end ) n_students_hs_not_f
        , count (distinct case when grade_band = 'HS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then student_id else null end ) n_students_hs_urg
        , count (distinct case when grade_band = 'HS' and race_group in ('white','asian','two_or_more_non_urg') then student_id else null end ) n_students_hs_not_urg
        , count (distinct case when grade_band = 'MS' and gender_group = 'f' then student_id else null end ) n_students_ms_f
        , count (distinct case when grade_band = 'MS' and gender_group in ('m','nb') then student_id else null end ) n_students_ms_not_f
        , count (distinct case when grade_band = 'MS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then student_id else null end ) n_students_ms_urg
        , count (distinct case when grade_band = 'MS' and race_group in ('white','asian','two_or_more_non_urg') then student_id else null end ) n_students_ms_not_urg
    from curriculum_counts
    where country = 'united states'
    group by 1
)
, us_metrics as (
    select
        school_year
        , round(cast(n_students_hs_f as decimal) / (n_students_hs_not_f + n_students_hs_f) * n_students_hs)::int as n_students_hs_f
        , round(cast(n_students_hs_urg as decimal) / (n_students_hs_not_urg + n_students_hs_urg) * n_students_hs)::int as n_students_hs_urg
        , round(cast(n_students_ms_f as decimal) / (n_students_ms_not_f + n_students_ms_f) * n_students_ms)::int as n_students_ms_f
        , round(cast(n_students_ms_urg as decimal) / (n_students_ms_not_urg + n_students_ms_urg) * n_students_ms)::int as n_students_ms_urg
    from us_metric_prep
    group by 1,2,3,4,5
)

, final as (
    select 
        all_counts.school_year
        ,  all_counts.country
        ,  all_counts.us_intl
        , coalesce(n_students, 0) as n_students
        , coalesce(n_students + round(0.4 * n_students_es)::int,0) as n_students_adj
        , coalesce(n_students_hs, 0) as n_students_hs
        , coalesce(n_students_ms, 0) as n_students_ms
        , coalesce(n_students_es, 0) as n_students_es
        , coalesce(round(n_students_es * 1.4)::int,0) as n_students_es_adj
        , n_students_hs_f
        , n_students_hs_urg
        , n_students_ms_f
        , n_students_ms_urg
    from all_counts
    left join grade_band_metrics
        on all_counts.school_year = grade_band_metrics.school_year
        and all_counts.country = grade_band_metrics.country
    left join us_metrics
        on all_counts.school_year = us_metrics.school_year
        and all_counts.country = 'united states'
)

select * 
from final
order by school_year desc, country