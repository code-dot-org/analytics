with 
user_project_storage as (
    select *
    from {{ ref('base_dashboard__user_project_storage_ids') }} )

select *
from user_project_storage