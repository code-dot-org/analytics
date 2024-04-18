/*

    The purpose of this intermidate table is "simply" to represent the cleaned, normalized and
    reshaped raw aggregate AP exam table.

    Note: computation of aggregates for groups: urg, non-urg, etc. is deferred to the mart model

    This model does two things:
    1. unions together all of the aggregated ap exam results
    2. computes the 'cdo_audit' aggregate group from code.org school-level exam results

*/

-- 1. Union together any/all existing agg reports data for every year availble.
--      note: 2017-2022 was imported in bulk in mar. 2024 in order to maintain historic data that
--      had previously been reported and before we started modeling in DBT.
--------
{% set years = ['2017_2022', '2023'] %}
{% set columns = [
    'exam_year::integer', 
    'pd_year::integer',
    'exam_group::text',
    'rp_id::integer',
    'exam::text', 
    'demographic_group::text', 
    'demographic_category::text', 
    'score_category::text', 
    'score_of::text', 
    'num_students::integer'
]%}
    
{% for year in years %}
select
    {% for column in columns %}
    {{ column }} {% if not loop.last %}, {% endif %}
    {% endfor %}
from {{ ref('stg_external_datasets__ap_agg_exam_results_' ~ year) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}

union all

-- 2. compose 'cdo_audit' agg reports from school_level_data
-- the order of these fields MUST match the order listed above.
select
    exam_year::integer, 
    null::integer as pd_year,
    'cdo_audit'::text as exam_group, -- in theory this should be run through the macro that normalizes these values
    null::integer as rp_id,
    exam::text, 
    demographic_group::text, 
    demographic_category::text, 
    score_category::text, 
    score_of::text, 
    sum(num_students)

from {{ ref('int_ap_school_level_exam_results') }}
{{ dbt_utils.group_by(9) }}