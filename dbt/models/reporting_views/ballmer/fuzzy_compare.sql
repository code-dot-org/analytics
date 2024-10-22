{#
    model: 
    auth: cory
    notes: Support tool for validating fuzzy matching between dim_schools and the AP ledger

    There are some cases where zip codes don't match to what's in dim_schools due to truncation (specifically, when a zip code is in the form 01234-5678, dim_schools will sometimes have it as 12345)
    #}

with schools as (
    select * from {{ref('dim_schools')}}
),

private_crosswalk as (
    select 
        lower(school_name) as school_name,
        lower(city) as city,
        state,
        left({{pad_zipcode('zip')}},5) as zip,
        {{pad_school_id('nces_id')}}                                   as school_id,                                                       
        {{ pad_ai_code('ai_code') }}                                    as ai_code
    from {{ ref('base_external_datasets__ap_crosswalk_2024') }} 
    where source = 'pvt school by hand'
)

select 
    private_crosswalk.ai_code as crosswalk_ai_code,
    private_crosswalk.school_id as school_id,
    private_crosswalk.school_name as crosswalk_name,
    lower(schools.school_name) as schools_name,
    private_crosswalk.city as crosswalk_city,
    lower(schools.city) as schools_city,
    private_crosswalk.zip as crosswalk_zip,
    schools.zip as schools_zip,
    crosswalk_name = schools_name as name_match,
    crosswalk_city = schools_city as city_match,
    crosswalk_zip = schools_zip as zip_match
from private_crosswalk
left join schools 
    on private_crosswalk.school_id = schools.school_id
    where name_match = false or city_match = false or zip_match = false