with 
source as (
    select * 
    from "dashboard"."dashboard_production"."regional_partners_school_districts"
),

renamed as (
    select
        regional_partner_id,
        school_district_id,
        course,
        workshop_days
    from source
)

select * 
from renamed