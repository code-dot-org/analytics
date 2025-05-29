with state_data as (
    select
        exam_year,
        ''                                          as pd_year,
        {{ ap_norm_exam_group('exam_group') }}      as reporting_group,
        ''                                          as rp_id,
         {{ ap_norm_exam_subject('exam') }}         as exam
    from {{ref('base_external_datasets__ap_state_data_2007_2024')}}
)

select * from state_data
