select school_year, count(*) as num_rows 
from {{ ref('dim_student_script_level_activity')}}
where school_year < '2024-25'
group by school_year
order by school_year desc