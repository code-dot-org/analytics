-- Note: this data is built using NCES data recevied

with 
school_stats_by_years as (
    select 
        *,

        -- pre-process these totals here
        sum(count_student_am 
            + count_student_hi
            + count_student_bl
            + count_student_hp)
        as total_urg_no_tr_students,

        sum(count_student_am
            + count_student_hi
            + count_student_bl
            + count_student_hp
            + count_student_tr)
         as total_urg_students,

        sum(count_student_am  
            + count_student_as  
            + count_student_hi  
            + count_student_bl  
            + count_student_wh  
            + count_student_hp
            + count_student_tr)
        as total_students_calculated

    from {{ ref('stg_dashboard__school_stats_by_years') }}
    
    {{ dbt_utils.group_by(40) }}
),

combined as (
    select 
        *,
        (
            (case 
                when is_stage_el = 1 
                then 'el' 
                else '__' 
            end ) ||
            (case 
                when is_stage_mi = 1 
                then 'mi' 
                else '__' 
            end ) ||
            (case 
                when is_stage_hi = 1 
                then 'hi' 
                else '__' 
            end ) 
        )                                                                           as school_level_simple,

        -- calculations 
        total_urg_students / nullif(total_students_calculated,0)::float             as urg_percent,

        total_urg_no_tr_students / nullif(total_students_calculated,0)::float       as urg_no_tr_percent,
        
        total_frl_eligible_students / nullif(total_students,0)::float               as frl_eligible_percent

    from school_stats_by_years
)

select 
    *
    , case 
        when frl_eligible_percent > 0.5
        then 1 else 0 
    end                                                                         as is_high_needs
    , case 
            when frl_eligible_percent < .25 then '1st quartile'
            when frl_eligible_percent < .50 then '2nd quartile'
            when frl_eligible_percent < .75 then '3rd quartile'
            when frl_eligible_percent <= 1 then '4th quartile'
            else null
        end                                                                     as frl_quartile
from combined 