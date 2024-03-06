/*
    This intermediate table unions together all the school-level AP exam results.
    It's designed to allow for different years of available data for CSP and CSA.

    The macro: build_ap_school_level_data_union_query unions all of the staging data together
    ASSUMING that staging data table names adhere to the established naming conventions.
    See the code for that macro to learn the expected column names and order for AP school level data.

    In theory, if (1) the staging table for the exam + year exists and (2) it is properly formed with
    the correct number and order of columns and (3) it is named according to the established naming convention, 
    then all the analytics engineer needs to do is add the appropriate year to the list-of-years for the 
    relavant exam (CSP or CSA) in the code below.

*/

-- 1. Union together any/all existing agg reports data for every year availble.
--      note: 2017-2022 was imported a
--------
{% set years = ['2017_2022', '2023'] %}
{% set columns = [
    'exam_year', 
    'pd_year',
    'exam_group',
    'rp_id',
    'exam', 
    'demographic_group', 
    'demographic_category', 
    'score_category', 
    'score_of', 
    'num_students'
]%}
    
{% for year in years %}
select
    {% for column in columns %}
    {{ column }} {% if not loop.last %}, {% endif %}
    {% endfor %}
from {{ ref('stg_external_datasets__ap_agg_exam_results_' ~ year) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}

-- compose agg reports from school_level_data

-- the order of these fields MUST match the order listed above.
select
    exam_year, 
    null as pd_year,
    'cdo_audit' as exam_group, -- in theory this should be run through the macro that normalizes these values
    null as rp_id,
    exam, 
    demographic_group, 
    demographic_category, 
    score_category, 
    score_of, 
    sum(num_students)

from {{ ref('int_ap_school_level_exam_results') }}
{{ dbt_utils.group_by(9) }}

