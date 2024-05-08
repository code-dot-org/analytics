with all_ledgers as (
    select * from {{ ref('stg_external_datasets__ap_ledgers_2017_2021') }}
    union all
    select * from {{ ref('stg_external_datasets__ap_ledgers_2022') }}
    union all
    select * from {{ ref('stg_external_datasets__ap_ledgers_2023') }}
)
select
   *
from all_ledgers
order by exam_year DESC, state, city, aicode
