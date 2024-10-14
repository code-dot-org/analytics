with teachers as (
    select * from dashboard.analysis.teachers
    WHERE trained=1
    and course_name in ('csp', 'csa')
    and school_year = '2023-24'
),

ledgers as (
    select * from {{ref ('dim_ap_ledgers')}}
    where school_year = '2023-24' --testing this restriction because it seems to be limiting the results
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
    LEFT JOIN ledgers on ledgers.school_id = teachers.school_id
    WHERE trained=1
    and course_name = 'csp'
    and ledgers.ai_code IS NOT NULL
    ),

csa_pd AS (
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
    LEFT JOIN ledgers on ledgers.school_id = teachers.school_id
    WHERE trained=1
    and course_name = 'csa'
    and ledgers.ai_code IS NOT NULL
    ),

csp_final AS (
    SELECT
    distinct
    'csp_all_time_pd' as label,
    ai_code,
    school_name,
    state
    --ledger_year
    FROM csp_pd
),

csa_final AS (
    SELECT
    distinct
    'csa_all_time_pd' as label,
    ai_code,
    school_name,
    state
    --ledger_year
    FROM csa_pd
),

combined as (
    select * from csp_final
    UNION all
    select * from csa_final
)

select * from combined
