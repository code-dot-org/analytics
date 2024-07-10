select
    user_id,
    sign_in_at::date activity_date,
    count(*) num_sign_ins
from {{ ref('stg_dashboard__sign_ins') }}
where trunc(sign_in_at) between '2022-01-01' and sysdate --remove this filter before publish, make incremental?
group by 1,2

