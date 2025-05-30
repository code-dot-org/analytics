/* Note that some of the normalization code is happening before the file is loaded*/

with state_data as (
    select
        exam_year,
        lower(exam_group)                          as reporting_group,
        {{ ap_norm_exam_subject('exam') }}         as exam,
        case 
            when gender = 'total' and race = 'total' then 'total'
            when gender = 'total' then race
            when race = 'total' then gender
            else concat(concat(gender, '_'), race)
            end                                     as demographic_group,
        case   
            when gender = 'total' and race = 'total' then 'total'
            when gender = 'total' then 'race'
            when race = 'total' then 'gender'
            else 'gender_race' end                 as demographic_category,
        {{ ap_norm_score_category('score') }},       
        num_taking                                 as num_students
    
    from {{ref('base_external_datasets__ap_state_data_2007_2024')}}
)

select * from state_data
