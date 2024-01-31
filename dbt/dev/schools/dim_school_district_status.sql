-- model: dim_school_district_status
-- scope: 
-- author: js
-- date: 2024-01-09


with 
school_status as (
    select school_id, school_year, max(status_code)
    from {{ ref('dim_school_status') }} 
    {{ dbt_utils.group_by(2) }}
),

