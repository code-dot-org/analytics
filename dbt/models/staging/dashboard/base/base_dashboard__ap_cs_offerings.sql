with 
source as (
      select * from {{ source('dashboard', 'ap_cs_offerings') }}
),

renamed as (
    select
        id as ap_cs_offerings_id,
        school_code,
        course,
        school_year,
        created_at,
        updated_at
    from source
)

select * from renamed