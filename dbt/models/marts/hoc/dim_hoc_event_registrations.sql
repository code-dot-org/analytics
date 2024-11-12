with 
forms as (
    select * 
    from {{ ref('dim_forms') }}
),

renamed as (
    select 
        form_id
        , form_kind
        , email 
        , hoc_year                                as cal_year
        , school_year
        , registered_at
        , trunc(registered_at)                    as registered_dt
        , event_type
        , email_pref
        , special_event_flag
        , review
        , city
        , state
        , country
    from forms 
    where form_category = 'hoc'
)

select * 
from renamed