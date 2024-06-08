-- staging co-teacher data
-- scope: invited co-teachers
with
source as (
    select *
    from {{ source('dashboard', 'section_instructors') }} )

select *
from source