with 
school_infos as (
    select * 
    from {{ ref('base_dashboard__school_infos')}}
)
, final as (
    select

        school_info_id,
        {{ pad_school_id('school_id') }}  as school_id,   
        school_name,
        school_type,
        school_other,
        school_district_id,
        school_district_other,
        school_district_name,
        
        full_address,
        state,
        zip,
        country,
        
        created_at,
        updated_at,
        validation_type
    from school_infos
)
select *
from final
