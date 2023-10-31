{# Notes:
Design: 1 row per teacher, school_year, churn_status
Logic: where teacher has active section, then:
    -- active retained: had an active section last SY and has an active section this SY (could be the same section ID, but needs to have 5+ active students both SYs)
    -- active reacquired: did not have an active section last SY, but does have one this SY
    -- active new: never has had an active section before, but has one this SY
    -- inactive churn: did not have an active section last SY and does not have an active section this SY
    -- inactive this year: had an active section last SY, does not have one this SY
    -- market: has a teacher account but has never had an active section 
#}

with 
teachers as (
    select * 
    from {{ ref('dim_teachers') }}
),

active_sections as (
    select *
    from {{ ref('int_active_sections') }}
    where teacher_id 
),

combined as (
    select 
        ss.school_year,
        ss.teacher_id,
        -- "status"
    from active_sections as s
        on ss.section_id = s.section_id
    {{ dbt_utils.group_by(2) }}
)

-- final as (
--     select 
--         school_year,
--         teacher_id, 
--         case when sum(is_active+lag(is_active)) = 2 then 'active - retained'
--              when sum(is_active+lag(is_active)) 
        
--         case
--             -- actives 
--             when is_active then case
--                 when is_active_previous_year then 'active - retained'
--                 when not is_active_previous_year then 'active - reacquired'
--                 when is_active_previous_year is null then 'active - new'
--                 else 'active - unknown' end 
--             -- inactives 
--             when not is_active then case
--                 when not is_active_previous_year then 'inactive - churn'
--                 when not is_active and is_active_previous_year then 'inactive - new'
--                 when is_active is null and is_active_previous_year is null then 'market'
--                 else 'inactive - unknown' end 
--             else 'uncategorized' end as churn_status
--     from combined
-- )

select * 
from combined