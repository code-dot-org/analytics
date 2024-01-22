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
        case when len(school_id) = 11 then lpad(school_id,12,'0') -- (bef) this is same padding that's done in schools. there's not an upstream place to do this.
        school_other,
        school_name,
        full_address,
        created_at,
        updated_at,
        validation_type


)

select * 
from school_infos