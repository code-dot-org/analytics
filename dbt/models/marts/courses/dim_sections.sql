{{
    config(
        materialized='incremental',
        unique_key='section_id'
    )
}}

with sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
    
    {% if is_incremental() %}

    where created_at > (select max(created_at) from {{ this }})

    {% endif %}
)

select * 
from sections


{# ),

section_status as (
    select *
    from {{ ref('int_section_status') }}
),

final as (
    select 
        -- section data
        sections.section_id,
        section_name,
        section_type,
        section_status.is_active,
        
        -- foreign keys
        user_id,
        
        -- timestamps
        created_at,
        updated_at
    from sections
    join section_status 
        on sections.section_id = section_status.section_id 
) #}