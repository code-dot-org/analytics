select school_year, count(*) as num_rows 
from {{ ref('dim_user_levels')}}
where school_year < '2024-25'
group by school_year