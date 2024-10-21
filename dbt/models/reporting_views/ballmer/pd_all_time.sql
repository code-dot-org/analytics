with teachers as (
    select
        user_id,
        school_year,
        school_info_id,
        {{ pad_school_id('school_id') }}  as school_id,
        school_name,
        course_name,
        started,
        started_at,
        trained,
        trained_this_year
        from dashboard.analysis.teachers
    WHERE trained=1
    and course_name in ('csp', 'csa')
    and school_year = '2023-24'
),

ledgers as (
    select * from {{ref ('dim_ap_ledgers')}}
    where school_year = '2023-24' 
),

csp_pd AS (
SELECT
    distinct
    teachers.school_year,
    course_name,
    trained,
    teachers.school_id,
    ai_code,
    ledgers.school_name,
    ledgers.state,
    ledgers.school_year as ledger_year
    FROM teachers
    LEFT JOIN ledgers on ledgers.school_id = teachers.school_id and ledgers.exam = teachers.course_name
    WHERE 
    course_name = 'csp' -- find all teachers who have ever been trained who teach csp this school year
    and ledgers.ai_code IS NOT NULL
)

, csa_pd AS (
    SELECT
    distinct
    teachers.school_year,
    course_name,
    trained,
    teachers.school_id,
    ai_code,
    ledgers.school_name,
    ledgers.state,
    ledgers.school_year as ledger_year
    FROM teachers
    LEFT JOIN ledgers on ledgers.school_id = teachers.school_id and ledgers.exam = teachers.course_name
    WHERE course_name = 'csa' -- find all teachers who have ever been trained who teach csa this school year
    and ledgers.ai_code IS NOT NULL
    )

, csp_final AS (
    SELECT
    distinct
    'csp_all_time_pd' as label,
    ai_code,
    school_name,
    state
    FROM csp_pd
), 

csa_final AS (
    SELECT
    distinct
    'csa_all_time_pd' as label,
    ai_code,
    school_name,
    state
    FROM csa_pd
),

combined as (
    select * from csp_final
    UNION all
    select * from csa_final
)

select * 
from combined