/*
   Union together all of the aceess report "megatables"

   ANNUAL TASK:
   0. (Assumption) you have created a new base table named: base_external_datasets__access_report_megatable_~year  for the year in question.
   1. add a new year to the years[] arary 
   2. Build the model
   3. Investigate data, ensure it passes tests 
   4. repeat until done.
*/
with all_data as (

    {% set years = ['2023','2024'] %} 

    {% for year in years %}
        select * from {{ ref('base_external_datasets__access_report_review_table_'~year) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)
select *
from all_data
where state not in ('AS', 'GU', 'MP', 'PR', 'VI') -- exclude territories
and teaches_cs_final not in ('unknown', 'E', 'flag') -- exclude schools with unknown CS status
and grade_levels != '______' -- exclude schools with no grade levels
and school_type in ('public', 'charter')