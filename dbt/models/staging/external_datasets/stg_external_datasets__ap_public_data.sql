with global_results as (
    select
        'college board'                         as source,
        exam_year,
        'global'                                as reporting_group,
        ''                                      as rp_id,
        {{ ap_norm_exam_subject('exam') }}      as exam,
        'total'                                 as demographic_category,
        'total'                                 as demographic_group,
        num_taking,
        round(num_taking * pct_passing,0)::int                                    as num_passing,
        pct_passing
    from {{ref('seed_public_ap_results')}}
)

select * from global_results
