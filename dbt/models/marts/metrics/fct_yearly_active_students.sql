-- model: fct_yearly_active_students

with active_students as (
    select * 
    from {{ ref('dim_active_students') }}
    where activity_date > {{ get_cutoff_date() }}
),

final as (
    select 
        school_year,
        us_intl,
        country,
        count(distinct student_id) as num_active_students
    from active_students
    {{ dbt_utils.group_by(3) }} )
    
select * 
from final 
order by school_year desc 