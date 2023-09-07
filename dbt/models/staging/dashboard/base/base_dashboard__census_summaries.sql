with 
source as (
      select * from {{ source('dashboard', 'census_summaries') }}
),

renamed as (
    select
        id as census_sumarries_id,
        school_id,
        school_year,
        teaches_cs,
        audit_data,
        created_at,
        updated_at
    from source
)

select * from renamed