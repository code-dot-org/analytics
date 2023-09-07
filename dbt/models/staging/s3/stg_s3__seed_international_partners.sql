with 
seed_international_partners as (
    select * 
    from {{ ref('base_s3__seed_international_partners') }}
)

select * from seed_international_partners