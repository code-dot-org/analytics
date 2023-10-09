with 
teachers as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=["user_id",
            "is_urg",
            "student_id"]) }}
    from {{ ('stg_dashboard__users') }}
    where teacher_id is not null 
)

select * 
from teachers 