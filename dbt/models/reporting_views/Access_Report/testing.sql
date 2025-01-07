select school_year, count(*) as num_rows 
from {{ ref('dim_user_levels')}}
group by school_year