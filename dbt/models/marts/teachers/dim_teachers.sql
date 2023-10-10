with 
teachers as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=["user_id",
            "is_urg",
            "student_id"]) }}
    from {{ ref('stg_dashboard__users') }}
    where teacher_id is not null 
),

user_geos as (
    select 
        user_id, 
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
    where user_id in (select teacher_id from teachers)
)

select 
    teachers.*, 
    user_geos.is_international
from teachers 
join user_geos 
    on teachers.teacher_id = user_geos.user_id
