with 

mappings as (
    select * 
    from {{ ref('seed_pl_grade_band_mappings') }}
)

select 
    lower(topic)        as topic
    , case  
        when lower(grade_band) = 'k-5' then 'k_5'
        when lower(grade_band) = '6-8' then '6_8'
        when lower(grade_band) = '9-12' then '9_12'
        when lower(grade_band) = 'skills-focused' then 'skills_focused'
        else lower(grade_band)
    end as grade_band
from mappings