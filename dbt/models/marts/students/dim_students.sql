with 
students as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=["user_id",
            "teacher_id",
            "age_years",                                  
            "studio_person_id",
            "school_info_id"]) }}
    from {{ ref('stg_dashboard__users') }}
    where student_id is not null 
),

user_geos as (
    select 
        user_id, 
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
),

school_years as (
    select * from {{ref('int_school_years')}}
),

final as (
select 
    students.*, 
    user_geos.is_international,
    sy.school_year created_at_school_year
from students 
join user_geos 
    on students.student_id = user_geos.user_id
left join school_years sy on students.created_at between sy.started_at and sy.ended_at
)

select * 
from final 