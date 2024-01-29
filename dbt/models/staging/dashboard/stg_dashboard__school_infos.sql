with 
school_infos as (
    select * 
    from {{ ref('base_dashboard__school_infos')}}
)
, final as (
    select

        school_info_id,
        country,
        school_type,
        zip,
        state,
        school_district_id,
        school_district_other,
        school_district_name,
        {{ pad_school_id('school_id') }}  as school_id,   
        school_other,
        school_name,
        full_address,
        created_at,
        updated_at,
        validation_type
    from school_infos
)
select *
from final
