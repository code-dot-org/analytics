with started as (
    select * from {{ref('csa_csp_csd_started')}}
)

, completed as (
    select * from {{ref('ballmer_heavy_user_students')}}
)

, students as (
    select * from {{ref('dim_students')}}
)

, race_groups_calc_started as (
    select 
        'ap_urg_calc_started' as dataset_name,
        (left(started.school_year,4)::integer + 1) as exam_year, -- calculates exam year from school year
        school_year,
        students.race_group,
        students.is_urg as urg,
        CASE WHEN students.race_group in ('black','hispanic','hawaiian','american_indian') then 1 else 0 end as bhnapi,
        CASE WHEN students.race_group in ('black','hispanic','hawaiian','american_indian','asian','white','other') then 1 else 0 end as single_race,
        CASE WHEN students.race_group = 'two_or_more_urg' then 1 else 0 end as tr_urg,
        CASE WHEN students.race_group = 'two_or_more_non_urg' then 1 else 0 end as tr_non_urg,
        CASE WHEN students.race_group in ('two_or_more_urg','two_or_more_non_urg') then 1 else 0 end as tr_tot,
        CASE WHEN (students.race_group in ('no_response','not collected') or students.race_group is null) then 0 else 1 end reporting_race,
        current_timestamp::date AS pulled_at,
        'using hydrone csa_csp_csd_started' AS notes,
        count(distinct(user_id)) num_students 
    from started
    join students
        on students.student_id = started.user_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

, race_groups_calc_completed as (
    select 
        'ap_urg_calc_completed' as dataset_name,
        (left(school_year,4)::integer + 1) as exam_year, -- calculates exam year from school year
        school_year,
        students.race_group,
        students.is_urg as urg,
        CASE WHEN students.race_group in ('black','hispanic','hawaiian','american_indian') then 1 else 0 end as bhnapi,
        CASE WHEN students.race_group in ('black','hispanic','hawaiian','american_indian','asian','white','other') then 1 else 0 end as single_race,
        CASE WHEN students.race_group = 'two_or_more_urg' then 1 else 0 end as tr_urg,
        CASE WHEN students.race_group = 'two_or_more_non_urg' then 1 else 0 end as tr_non_urg,
        CASE WHEN students.race_group in ('two_or_more_urg','two_or_more_non_urg') then 1 else 0 end as tr_tot,
        CASE WHEN (students.race_group in ('no_response','not collected') or students.race_group is null) then 0 else 1 end reporting_race,
        current_timestamp::date AS pulled_at,
        'using hydrone csa_csp_completed' AS notes,
        count(distinct(user_id)) num_students 
    from completed
    join students
        on students.student_id = completed.user_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

, unioned_calcs as (
    select
        dataset_name,
        exam_year,
        school_year,
        SUM(CASE WHEN bhnapi = 1 THEN num_students ELSE 0 END) AS bhnapi_students,
        SUM(CASE WHEN single_race = 1 THEN num_students ELSE 0 END) AS single_race_students,
        SUM(CASE WHEN tr_urg = 1 THEN num_students ELSE 0 END) AS tr_urg_students,
        SUM(CASE WHEN tr_tot = 1 THEN num_students ELSE 0 END) AS tr_tot_students,
        (single_race_students::float * tr_urg_students::float) / 
        NULLIF((bhnapi_students::float * tr_tot_students), 0) AS cdo_multiplier,
        notes,
        pulled_at
    from
        race_groups_calc_started
    group by
        dataset_name,
        exam_year,
        school_year,
        notes,
        pulled_at

    union all

    select
        dataset_name,
        exam_year,
        school_year,
        SUM(CASE WHEN bhnapi = 1 THEN num_students ELSE 0 END) AS bhnapi_students,
        SUM(CASE WHEN single_race = 1 THEN num_students ELSE 0 END) AS single_race_students,
        SUM(CASE WHEN tr_urg = 1 THEN num_students ELSE 0 END) AS tr_urg_students,
        SUM(CASE WHEN tr_tot = 1 THEN num_students ELSE 0 END) AS tr_tot_students,
        (single_race_students::float * tr_urg_students::float) / 
        NULLIF((bhnapi_students::float * tr_tot_students), 0) AS cdo_multiplier,
        notes,
        pulled_at
    from
        race_groups_calc_completed
    group by
        dataset_name,
        exam_year,
        school_year,
        notes,
        pulled_at
    )

, final as (
select *
from unioned_calcs
order by
    exam_year,
    school_year,
    dataset_name
)

select * 
from final
