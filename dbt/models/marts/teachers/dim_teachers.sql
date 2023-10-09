with 
teachers as (
    select 
    {{ dbt_utils.star(
        from=ref('dim_users'),
        except=["user_id",
            "is_urg",
            "student_id"]) }}
    from {{ ('dim_users') }}
    where teacher_id is not null 
)

select * 
from teachers 