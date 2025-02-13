with 
source as (
    select * 
    from {{ source('dashboard', 'course_scripts') }}
)

select * 
from source  