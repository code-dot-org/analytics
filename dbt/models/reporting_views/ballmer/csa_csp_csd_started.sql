with duca as
(
    select * 
    from {{ref('dim_user_course_activity')}}
    where
        user_type = 'student'
        and course_name in ('csa','csp','csd')
)
    
, final as 
(
    select 
        course_name
        , user_id
        , school_year
        , first_activity_at	as started_at
        , last_activity_at	as last_progress_at
        , num_levels		as lvl_cnt
    from duca 
)

select * from final