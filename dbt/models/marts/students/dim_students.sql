with 
students as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=["user_id",
            "teacher_id"]) }}
    from {{ ref('stg_dashboard__users') }}
    where student_id is not null 
)

select * 
from students 