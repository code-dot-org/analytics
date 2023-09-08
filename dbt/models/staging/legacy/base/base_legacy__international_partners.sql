with 
source as (
      select * 
      from {{ source('legacy', 'seed_international_partners') }}
)

select * from source