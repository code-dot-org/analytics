with 
students as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=["user_id",
            "teacher_id"]) }}
    from {{ ref('stg_dashboard__users') }}
    where student_id is not null 
),

user_geos as (
    select 
        user_id, 
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
    where user_id in (select student_id from students)
)

select 
    students.*, 
    user_geos.is_international
from students 
join user_geos 
    on students.student_id = user_geos.user_id