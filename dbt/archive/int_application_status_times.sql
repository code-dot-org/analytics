with 

pd_applications_status_logs as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_applications_status_logs') }}
),

days_in_status as (
    select 
        logs_1.pd_application_id
        , logs_1.application_status
        , case 
            when logs_2.changed_status_dt is not null 
            then datediff('day', logs_1.changed_status_dt, logs_2.changed_status_dt) 
            else datediff('day', logs_1.changed_status_dt, current_date) 
        end                                                                     as days_in_status  
    from pd_applications_status_logs                                            as logs_1
    left join pd_applications_status_logs                                       as logs_2 
        on logs_1.change_order = logs_2.change_order - 1
        and logs_1.pd_application_id = logs_2.pd_application_id
    order by logs_1.pd_application_id
)

select * 
from days_in_status
pivot (
    avg(days_in_status) 
    for application_status in (
        'pending' as days_in_pending
        ,'unreviewed' as days_in_unreviewed
        ,'incomplete' as days_in_incomplete
        ,'pending_space_availability' as days_in_pending_space_availability
        ,'awaiting_admin_approval' as days_in_awaiting_admin_approval
    )
)