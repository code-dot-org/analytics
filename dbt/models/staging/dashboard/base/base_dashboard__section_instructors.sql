<<<<<<< HEAD
=======
-- staging co-teacher data
-- scope: invited co-teachers
>>>>>>> 11b3dbeb0e26d163f791230fbdb3d913dd02615e
with
source as (
    select *
    from {{ source('dashboard', 'section_instructors') }}
),

renamed as (
    select 
        id as section_instructor_id,
        instructor_id,
        section_id,
        invited_by_id,
        status,
        created_at,
        updated_at,
        deleted_at
    from source)

select *
from renamed