-- staging co-teacher data
-- scope: invited co-teachers
with
section_instructors as (
    select * 
    from {{ ref('base_dashboard__section_instructors') }}
    where invited_by_id is not null)

select * 
<<<<<<< HEAD
from section_instructors
=======
from section_instructors
>>>>>>> 11b3dbeb0e26d163f791230fbdb3d913dd02615e
