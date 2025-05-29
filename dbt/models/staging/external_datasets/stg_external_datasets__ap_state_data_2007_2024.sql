with state_data as (
    select
        exam_year,
        lower(exam)                                as exam,
        gender,
        race,
        lower(exam_group)                          as exam_group,
        num_taking,
        score
    from {{ref('base_external_datasets__ap_state_data_2007_2024')}}
)

, normalized as (
    select 
        exam_year,
        {{ ap_norm_exam_subject('exam') }}         as exam,
        gender,
        race,
        {{ ap_norm_exam_group('exam_group') }}      as reporting_group,
        num_taking,
        {{ ap_norm_score_category('score')}}

    from {{ref('base_external_datasets__ap_state_data_2007_2024')}}
)

select * from normalized
