/*
    This model summarizes all aggregate AP exam results data into num_taking and num_passing
    AND performs the "external URG" calculations necessary to extrapolate the number of URG taking and passing the exam.

Edits:
- CK, May 2025 - some aggregates have totals that are not the sum of the individual score counts. This is because scores are not provided when the group size is <5 students. Therefore, if a denominator (total) is provided, we need to use that as num_taking rather than summing up 1,2,3,4,5. If these two scores don't match, don't show pct_passing
    
*/
with agg_exam_results as (
    select * from {{ ref('int_ap_agg_exam_results_calculate_race_no_response') }}
    union all
    select * from {{ ref('int_ap_agg_exam_results_calculate_agg_race_groups') }}
)

-- global data is loaded directly and does not go through the onboarding process for other aggregates because we only have pass rates, not the individual score counts
, global_data as (
    select * from {{ref('stg_external_datasets__ap_public_data')}}
)

, cdo_tr_multipliers as (
    select
        *
    from {{ref('seed_ap_tr_urg_multiplier')}}
    where 
        dataset_name = 'ap_urg_calc_started' -- this set produced values closest to what we reported in the past
        --dataset_name = 'ap_urg_calc_completed'

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
        sum(case when score_of is null
                then num_students else 0 end)                                                    as num_taking_provided,                       
                 --including this field to make QC easier
        sum(case when score_of in (1,2,3,4,5) then num_students else 0 end)                      as num_taking,
        sum(case when score_of in (3,4,5) then num_students else 0 end)                          as num_passing,
        case
            when num_taking_provided = num_taking and num_taking > 0 --if provided total = sum of scores, calculate pct pass
                then num_passing::float / num_taking::float
            when num_taking_provided is null and num_taking > 0 --if scores exist and total does not, calculate pct pass
                then num_passing::float /num_taking::float
            else --if scores don't match or no scores provided, don't calculate pct pass
                null end                                                                           as pct_passing 
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
        where sr_total = (bhnapi + wh_as_other)

    */
    select
        'calculated'                                                                                        as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg'                                                                                          as demographic_category,
        'tr_urg'                                                                                            as demographic_group,

        (
            (sum(
                case 
                    when a.demographic_group = 'bhnapi' then a.num_taking::float 
                    else 0 
                end
                ) *
            sum(
                case 
                    when a.demographic_group = 'two_or_more' then a.num_taking::float 
                    else 0 
                end
                )
            ) /
            nullif(
                sum(
                    case 
                        when a.demographic_group in ('bhnapi', 'wh_as_other') then a.num_taking::float 
                        else 0 
                    end), 0) --AS ext_multiplier,
        )
        * max(cdo.cdo_multiplier)                                                                           as num_taking_calc,

        (
            (
                sum(
                    case
                        when a.demographic_group = 'bhnapi' then a.num_passing::float 
                        else 0 
                    end) *
            sum(
                case 
                    when a.demographic_group = 'two_or_more' then a.num_passing::float 
                    else 0 
                end)) /
            nullif(
                sum(
                    case 
                        when a.demographic_group in ('bhnapi','wh_as_other') then a.num_passing::float 
                        else 0 
                    end), 0) --AS ext_multiplier,
        )
        * max(cdo.cdo_multiplier)                                                                           as num_passing_calc,

        coalesce(num_passing_calc::float / nullif(num_taking_calc::float, 0), 0)                            as pct_passing_calc

    from all_summary a
    
    left join cdo_tr_multipliers cdo 
        on a.exam_year = cdo.exam_year
    
    where a.demographic_group in ('bhnapi','two_or_more','wh_as_other') 

    {{ dbt_utils.group_by(7) }}  
)


, tr_non_urg as ( --tr_non_urg = two_or_more minus tr_urg
    select
        'calculated'                                                                                        as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg'                                                                                          as demographic_category,
        'tr_non_urg'                                                                                        as demographic_group,

        (
            sum(
                case 
                    when a.demographic_group = 'two_or_more' then a.num_taking 
                    else 0 
                end) -
            sum(
                case 
                    when a.demographic_group = 'tr_urg' then a.num_taking 
                    else 0 
                end)
        )                                                                                               as num_taking_calc,

        (
            sum(
                case 
                    when a.demographic_group = 'two_or_more' then a.num_passing 
                    else 0 
                end) -
            sum(
                case 
                    when a.demographic_group = 'tr_urg' then a.num_passing 
                    else 0 
                end)
        ) as num_passing_calc,

        coalesce(num_passing_calc::float / nullif(num_taking_calc::float, 0), 0)                        as pct_passing_calc

    from (
        select
            source,
            exam_year,
            reporting_group,
            rp_id,
            exam,
            demographic_category,
            demographic_group,
            num_taking,
            num_passing,
            pct_passing
        from all_summary 
        where demographic_group in ('two_or_more') -- not sure if this adds efficiency or not
        union all 
        select * 
        from tr_urg
    )                                                                                                   as a
    {{dbt_utils.group_by(7)}}
)

, urg_final as ( -- urg = bhnapi + tr_urg
    select
        'calculated'                                                                                    as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'urg_final'                                                                                     as demographic_category,
        'urg'                                                                                           as demographic_group,

        sum(
            case 
                when a.demographic_group in ('bhnapi','tr_urg') then a.num_taking 
                else 0 
            end)                                                                                        as num_taking_calc,        
        sum(
            case 
                when a.demographic_group in ('bhnapi','tr_urg') then a.num_passing 
                else 0 
            end)                                                                                        as num_passing_calc,

        coalesce(num_passing_calc::float / nullif(num_taking_calc::float, 0), 0)                        as pct_passing_calc

    from (
        select 
            source,
            exam_year,
            reporting_group,
            rp_id,
            exam,
            demographic_category,
            demographic_group,
            num_taking,
            num_passing,
            pct_passing
        from all_summary 
        where demographic_group in ('bhnapi') 
        union all 
        select * from tr_urg
    ) as a
    {{dbt_utils.group_by(7)}}
)

, non_urg_final as ( -- non_urg = wh_as_other + tr_non_urg
    select
        'calculated'                                                                                        as source,
        a.exam_year,
        reporting_group,
        rp_id,
        exam,
        'urg_final'                                                                                         as demographic_category,
        'non_urg'                                                                                           as demographic_group,

        sum(
            case 
                when a.demographic_group in ('wh_as_other','tr_non_urg') then a.num_taking 
                else 0 
            end)                                                                                            as num_taking_calc,        
        sum(
            case 
                when a.demographic_group in ('wh_as_other','tr_non_urg') then a.num_passing 
                else 0 
            end)                                                                                            as num_passing_calc,

        coalesce(num_passing_calc::float / nullif(num_taking_calc::float, 0), 0)                            as pct_passing_calc

    from (
        select 
            source,
            exam_year,
            reporting_group,
            rp_id,
            exam,
            demographic_category,
            demographic_group,
            num_taking,
            num_passing,
            pct_passing
        from all_summary where demographic_group in ('wh_as_other') 
        union all 
        select * from tr_non_urg
    )                                                                                                       as a 
    {{dbt_utils.group_by(7)}}
)

, urg_final_race_no_response as ( --make a copy of race_no_response to stick into the urg_final category so the category sums up properly
    select 
        source,
        exam_year,
        reporting_group,
        rp_id,
        exam,
        'urg_final'                                                                                         as demographic_category,
        demographic_group,
        num_taking,
        num_passing,
        pct_passing  
    from all_summary

    where demographic_category = 'race' 
    and demographic_group = 'race_no_response'
)

, final as (
    select 
        source,
        exam_year,
        reporting_group,
        rp_id,
        exam,
        demographic_category,
        demographic_group,
        num_taking,
        num_passing,
        pct_passing
    from all_summary
    union all
    select * from tr_urg
    union all
    select * from tr_non_urg
    union all
    select * from urg_final
    union all
    select * from non_urg_final
    union all
    select * from urg_final_race_no_response
    union all 
    select * from global_data
)

select
    source,
    exam_year,
    reporting_group,
    rp_id,
    exam,
    demographic_category,
    demographic_group,
    coalesce(num_taking,0)                                                                                  as num_taking,
    case                 --remove passing counts if they don't match quality flags
        when pct_passing is null then null
        else num_passing end                                                                                   as num_passing,
    pct_passing          --pct_passing should not be coalesced                                                                      
from final
where exam in ('csa','csp')
and reporting_group in ('csp_pd_alltime','csa_pd_alltime','national','global','csp_audit','csa_audit','csp_users','csa_users','csa_users_and_audit','csp_users_and_audit',
'csa_afe_eligible_schools','csp_afe_eligible_schools') --excludes all the RP reporting we no longer use
