with
pd_applications as (
    select * 
    from {{ ref('base_dashboard_pii__pd_applications') }}
)

select 
    pd_application_id
    , application_guid
    , teacher_id
    , lower(application_type)                                           as application_type 
    , left(application_year,4)                                          as cal_year
    , left(application_year,5) + substring(application_year, 8, 3)      as school_year  
    , regional_partner_id
    , case 
        when 
            current_status like '%accepted%' 
            or current_status in (
                'paid'
                ,'registration_sent'
            ) then 'accepted'
        else current_status 
    end                                                                 as current_status 
    , course_name
    , applied_at
    , accepted_at
        , case 
        when json_extract_path_text(
            properties,'principal_approval_not_required'
        ) = '' then 1 
        else 0 
    end                                                                 as admin_approval_required
    , case 
        when json_extract_path_text(
            form_data,'doYouApprove'
        ) = 'Yes' then 1 
        else 0
    end                                                                 as admin_approval_received
    , case 
        when 
            teacher_id is not null 
            and applied_at is not null 
        then 1
        else 0
    end                                                                 as submitted
    , case 
        when 
            teacher_id is not null 
            and applied_at is not null 
        then 'app submitted'
        when 
            teacher_id is not null 
            and applied_at is null 
        then 'app saved'
        else 'unknown'
    end                                                                 as submission_status
    , case 
        when current_status in (
            'accepted'
            ,'accepted_no_cost_registration'
            ,'accepted_not_notified'
            ,'accepted_notified_by_partner'
            ,'paid','registration_sent')
        then 1
        else 0
    end                                                                 as accepted   
    , json_extract_path_text(form_data, 'school')                       as user_entered_school
    , status_timestamp_change_log
    , json_extract_path_text(
        json_extract_path_text(
            response_scores
            , 'meets_scholarship_criteria_scores'
            )
        , 'free_lunch_percent'
        )                                                               as scholarship_frl
    , json_extract_path_text(
        json_extract_path_text(
            response_scores
            , 'meets_scholarship_criteria_scores'
        )
    ,'underrepresented_minority_percent'
    )                                                                   as scholarship_urg
    , json_extract_path_text(form_data, 'howHeard')                     as how_heard
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Code.org website%' then 1 
        else 0 
    end                                                                 as how_heard_code_website
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Code.org email%' then 1 
        else 0 
    end                                                                 as how_heard_email
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Regional Partner website%' then 1 
        else 0 
    end                                                                 as how_heard_rp_website
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Regional Partner email%' then 1 
        else 0 
    end                                                                 as how_heard_rp_email
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Regional Partner event or workshop%' then 1 
        else 0 
    end                                                                 as how_heard_rp_event_workshop
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Teacher%' then 1 
        else 0 
    end                                                                 as how_heard_teacher
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%District administrator%' then 1 
        else 0 
    end                                                                 as how_heard_administrator
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Conference%' then 1 
        else 0 
    end                                                                 as how_heard_conference
    , case 
        when json_extract_path_text(
            form_data, 'howHeard'
        ) like '%Other:%' then 1 
        else 0 
    end                                                                 as how_heard_other
from pd_applications