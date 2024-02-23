-- staging co-teacher data
-- scope: invited co-teachers
with
section_instructors as (
    select * 
    from {{ ref('base_dashboard__section_instructors') }}
    where invited_by_id is not null)

select * 
from section_instructors