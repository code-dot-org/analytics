/* 
    This is a dump of all the (slightly modified) AP state aggregates from 2007-2024. It is traditionally used in the Access Report. The 2023 and 2024 files have AP scores as well as num_taking, but the CB has hidden score results for any demographic group that has fewer than 5 responses. 
*/ 

with ap_data AS (
    select * from {{ source('external_datasets','ap_state_data_2007_2024') }}
)

SELECT * FROM ap_data