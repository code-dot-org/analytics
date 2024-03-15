-- staging co-teacher data
-- scope: invited co-teachers
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