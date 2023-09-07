with 
international_opt_ins as (
    select 
        {{ dbt_utils.star(
            from=ref('stg_dashboard_pii__pd_international_opt_ins'), 
            except='form_data') }},
        rank() over(
            partition by user_id,
                workshop_organizer,
                workshop_course,
                form_submitted_at
            desc) 
        as form_rnk
    from {{ ref('stg_dashboard_pii__pd_international_opt_ins')}}
), 

international_partners as (
    select * 
    from {{ ref('seed_international_partners') }}
),

international_affiliates as (
    select distinct 
        partner_id,
        contact_email,
        display_name,
        case
            when workshop_organizers = '' then 'n/a'
            else workshop_organizers
        end as workshop_organizer,
        case
            when partner_type = '' then 'public'
            else partner_type
        end as partner_type
    from international_partners
),

international_contact_info as (
    select *
    from {{ ref('seed_international_contact_info')}}
),

countries as (
    select * 
    from {{ ref('seed_countries')}}
),

users as (
    select * 
    from {{ ref('stg_dashboard__users') }}
    where user_id in (select user_id from international_partners)
),

combined as (
    select 
        ioi.*,
        ici.contact_name,
        ia.*
    from international_opt_ins as ioi 
    left join international_affiliates as ia 
        on ioi.workshop_organizers = ia.workshop_organizer
    left join international_contact_info as ici 
        on ioi.contact_email = ici.contact_email
    left join countries 
        on lower(ioi.school_country) = lower(countries.alt_name)
    where ioi.form_rnk = 1
)

select * 
from final 
