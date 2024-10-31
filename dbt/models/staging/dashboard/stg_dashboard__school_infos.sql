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
        upper(state) as state,
        school_district_id,
        school_district_other,
        lower(school_district_name) as school_district_name,
        {{ pad_school_id('school_id') }}  as school_id,   
        school_other,
        lower(school_name) as school_name,
        lower(full_address) as full_address,
        created_at,
        updated_at,
        validation_type
    from school_infos
)
select *
from final
