with 
students as (
    select 
    {{ dbt_utils.star(
        from=ref('dim_users'),
        except=["user_id",
            "teacher_id"]) }}
    from {{ ('dim_users') }}
    where student_id is not null 
)

select * 
from students 