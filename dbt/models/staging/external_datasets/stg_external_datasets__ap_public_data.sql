select
    exam_year,
    ''                              as pd_year,
    exam_group,
    ''                              as rp_id,
    exam,
    total_5,
    total_4,
    total_3,
    total_2,
    total_1,
    total_all
from {{ref('seed_public_ap_results')}}