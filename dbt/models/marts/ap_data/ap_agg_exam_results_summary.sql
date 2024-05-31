with agg_exam_results as (
    select * from {{ ref('int_ap_agg_exam_results') }}
    union all
    select * from {{ ref('int_ap_agg_exam_results_calculated_groups') }}
)
, cdo_tr_multipliers as (
    select
        *
    from {{ref('seed_ap_tr_urg_multiplier')}}
)
, all_summary as (
    select
        source,
        exam_year,
        reporting_group,
        rp_id,
        exam,
        demographic_category,
        demographic_group,
        -- SUM(CASE WHEN score_category = 'total' THEN num_students ELSE 0 END) AS total_students, --used to sanity check. scores 1-5 should = total
        SUM(CASE WHEN score_of IN (1,2,3,4,5) THEN num_students ELSE 0 END) AS num_taking,
        SUM(CASE WHEN score_of IN (3,4,5) THEN num_students ELSE 0 END) AS num_passing,
        COALESCE(num_passing::float / NULLIF(num_taking::float, 0), 0) AS pct_passing --prevent division by 0
    from agg_exam_results
    {{ dbt_utils.group_by(7) }}  
    order by
        source,
        demographic_category,
        demographic_group
)
, tr_urg as (
    /*
        The forumla here is:

        tr_urg = [(bhnapi * tr_total) / sr_total ] * cdo_multiplier
        note that sr_total = (bhnapi + wh_as_other)

    */
    select
        'caclulated' as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg' as demographic_category,
        'tr_urg' as demographic_group,

        (
            (SUM(CASE WHEN a.demographic_group = 'bhnapi' THEN a.num_taking::float ELSE 0 END) *
            SUM(CASE WHEN a.demographic_group = 'two_or_more' THEN a.num_taking::float ELSE 0 END)) /
            NULLIF(sum(case when a.demographic_group in ('bhnapi', 'wh_as_other') THEN a.num_taking::float ELSE 0 END), 0) --AS ext_multiplier,
        )
        * max(cdo.cdo_multiplier) AS num_taking_calc,

        (
            (SUM(CASE WHEN a.demographic_group = 'bhnapi' THEN a.num_passing::float ELSE 0 END) *
            SUM(CASE WHEN a.demographic_group = 'two_or_more' THEN a.num_passing::float ELSE 0 END)) /
            NULLIF(SUM(CASE WHEN a.demographic_group in ('bhnapi','wh_as_other') THEN a.num_passing::float ELSE 0 END), 0) --AS ext_multiplier,
        )
        * max(cdo.cdo_multiplier) AS num_passing_calc,

        COALESCE(num_passing_calc::float / NULLIF(num_taking_calc::float, 0), 0) AS pct_passing_calc

    from all_summary a
    left join cdo_tr_multipliers cdo on a.exam_year = cdo.exam_year
    where a.demographic_group in ('bhnapi','two_or_more','wh_as_other') -- not sure if this adds efficiency or not
    {{ dbt_utils.group_by(7) }}  
)
, tr_non_urg as ( --tr_non_urg = two_or_more minus tr_urg
    select
        'caclulated' as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg' as demographic_category,
        'tr_non_urg' as demographic_group,

        (
            SUM(CASE WHEN a.demographic_group = 'two_or_more' THEN a.num_taking ELSE 0 END) -
            SUM(CASE WHEN a.demographic_group = 'tr_urg' THEN a.num_taking ELSE 0 END)
        ) as num_taking_calc,

        (
            SUM(CASE WHEN a.demographic_group = 'two_or_more' THEN a.num_passing ELSE 0 END) -
            SUM(CASE WHEN a.demographic_group = 'tr_urg' THEN a.num_passing ELSE 0 END)
        ) as num_passing_calc,

        COALESCE(num_passing_calc::float / NULLIF(num_taking_calc::float, 0), 0) AS pct_passing_calc

    from (
        select * from all_summary where demographic_group in ('two_or_more') -- not sure if this adds efficiency or not
        union all 
        select * from tr_urg
    ) as a
    {{dbt_utils.group_by(7)}}
)
, urg_final as ( -- urg = bhnapi + tr_urg
    select
        'caclulated' as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'urg_final' as demographic_category,
        'urg' as demographic_group,

        SUM(CASE WHEN a.demographic_group in ('bhnapi','tr_urg') THEN a.num_taking ELSE 0 END) as num_taking_calc,        
        SUM(CASE WHEN a.demographic_group in ('bhnapi','tr_urg') THEN a.num_passing ELSE 0 END) as num_passing_calc,

        COALESCE(num_passing_calc::float / NULLIF(num_taking_calc::float, 0), 0) AS pct_passing_calc

    from (
        select * from all_summary where demographic_group in ('bhnapi') -- not sure if this adds efficiency or not
        union all 
        select * from tr_urg
    ) as a
    {{dbt_utils.group_by(7)}}
)
, non_urg_final as ( -- non_urg = wh_as_other + tr_non_urg
    select
        'caclulated' as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'urg_final' as demographic_category,
        'non_urg' as demographic_group,

        SUM(CASE WHEN a.demographic_group in ('wh_as_other','tr_non_urg') THEN a.num_taking ELSE 0 END) as num_taking_calc,        
        SUM(CASE WHEN a.demographic_group in ('wh_as_other','tr_non_urg') THEN a.num_passing ELSE 0 END) as num_passing_calc,

        COALESCE(num_passing_calc::float / NULLIF(num_taking_calc::float, 0), 0) AS pct_passing_calc

    from (
        select * from all_summary where demographic_group in ('wh_as_other') -- not sure if this adds efficiency or not
        union all 
        select * from tr_non_urg
    ) as a 
    {{dbt_utils.group_by(7)}}
)
, final as (
    select * from all_summary
    union all
    select * from tr_urg
    union all
    select * from tr_non_urg
    union all
    select * from urg_final
    union all
    select * from non_urg_final
)
select * from final