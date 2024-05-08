with level_concept_difficulties as (
    select * from {{ ref('stg_dashboard__level_concept_difficulties') }}
)
, course_structure as (
    select * from {{ref('dim_course_structure')}}
)
, final as (
    select
    cs.script_name,
    cs.stage_number,
    cs.stage_name, 
    cs.level_name,
    cs.level_script_order,
    cs.level_id,
    cs.course_name_true course_family,
    --lcd.level_id,
    --lcd.created_at,
    --lcd.updated_at,
    case when lcd.level_id is null then 'no' else 'yes' end has_lcd_mapping,
    lcd.sequencing,
    lcd.debugging,
    lcd.repeat_loops,
    lcd.repeat_until_while,
    lcd.for_loops,
    lcd.events,
    lcd.variables,
    lcd.functions,
    lcd.functions_with_params,
    lcd.conditionals

from course_structure cs 
left join level_concept_difficulties lcd on lcd.level_id = cs.level_id
)
select *
from final