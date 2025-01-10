with students as (
    select * from {{ref('dim_students')}}
    where created_at_school_year = '2024-25'
),

final as (
    select
        race_group,
        count(*) as count_race_group
    from students
    group by race_group
)

select * from 
final
