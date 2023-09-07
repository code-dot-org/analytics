with 
source as (
      select * from {{ source('analytics', 'seed_international_partners') }}
),

select * from source