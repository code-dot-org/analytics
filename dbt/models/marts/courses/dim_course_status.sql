with 
teachers as (
    select school_id,
        school_year,
        course_name,
        count(distinct case when started_at is not null then teacher_user_id end) as num_teachers_started,
        min(started_at) as first_started_at,
        dense_rank() over(partition by school_id, course_name order by school_year asc) as sequence_num
        dense_rank() over(partition by school_id, course_name order by school_year desc) as sequence_num_inv
    from {{ ref('dim_teachers') }}
    where started_at is not null 
    {{ dbt_utils.dbt_utils.group_by(3)}}
),

students as (
    select school_id,
        school_year,
        course_name,
        count(distinct case when started_at is not null then teacher_user_id end) as num_teachers_started,
        min(started_at) as first_started_at,
        dense_rank() over(partition by school_id, course_name order by school_year asc) as sequence_num
        dense_rank() over(partition by school_id, course_name order by school_year desc) as sequence_num_inv
    from {{ ref('dim_students') }}
    where started_at is not null 
    {{ dbt_utils.dbt_utils.group_by(3)}}
),


