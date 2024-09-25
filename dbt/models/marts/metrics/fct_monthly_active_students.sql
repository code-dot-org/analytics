{# 
    model: fct_active_students_monthly
    changelog:
    author      version date        comments
    js          2.0    2024-09-17   init 
    ""          2.1     ""          removing anonymous users from scope
#}

with active_students as (
    select * 
    from {{ ref('dim_active_students') }}
    where activity_date > {{ get_cutoff_date() }}
),

final as (
    select 
        date_trunc('month',activity_date) as activity_month,
        us_intl,
        country,
        count(distinct student_id) as num_active_students
    from active_students
    {{ dbt_utils.group_by(3) }} )
    
select * 
from final 
order by activity_month desc 