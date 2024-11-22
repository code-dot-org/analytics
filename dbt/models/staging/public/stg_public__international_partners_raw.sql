with international_partners_raw as (
    select * 
    from {{ ref ('base_public__international_partners_raw') }} )

select * from international_partners_raw