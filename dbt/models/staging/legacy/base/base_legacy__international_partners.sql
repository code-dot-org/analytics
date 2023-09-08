with 
source as (
      select * 
      from {{ source('legacy', 'seed_international_partners') }}
<<<<<<< HEAD
),
=======
)
>>>>>>> 7da41cceb6a5c29e92f20567b06cdb6edd69bfd5

select * from source