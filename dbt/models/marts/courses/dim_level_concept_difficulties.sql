with 
level_concept_difficulties as (
    select * 
    from {{ ref('stg_dashboard__level_concept_difficulties') }}
), 

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
), 

final as (
    select
        cs.script_name,
        cs.stage_number,
        cs.stage_name, 
        cs.level_name,
        cs.level_script_order,
        cs.level_id,
        cs.course_name_true as course_family,

        case when lcd.level_id is null 
            then 1 else 0 end as has_lcd_mapping,
        
        lcd.sequencing,
        lcd.debugging,
        lcd.repeat_loops,
        lcd.repeat_until_while,
        lcd.for_loops,
        lcd.events,
        lcd.variables,
        lcd.functions,
        lcd.functions_with_params,
        lcd.conditionals,
        lcd.created_at,
        lcd.updated_at

from course_structure as cs 
left join level_concept_difficulties as lcd 
     on cs.level_id = lcd.level_id )


select *
from final 