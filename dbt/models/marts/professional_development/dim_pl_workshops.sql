with 

int_pl_workshops as (

    select 
        pl_workshop_id,
        pl_organizer_id,
        pl_regional_partner_id,
        school_year,
        workshop_subject,
        workshop_started_at,
        workshop_ended_at,
        participant_group_type,
        is_virtual,
        is_byow,
        num_teachers_enrolled,
        num_teachers_attended,
        pct_teachers_attended,
        num_sessions,
        avg_sessions_attended,
        pct_sessions_attended,
        listagg(distinct topic, ', ') within group (order by pl_workshop_id) as topics,
        listagg(distinct grade_band, ', ') within group (order by pl_workshop_id) as grade_bands

    from {{ ref('int_pl_workshops') }}
    {{ dbt_utils.group_by(16) }}
) 

select * 
from int_pl_workshops

