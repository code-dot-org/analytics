with 
opt_ins as (
    select *,
        rank() over(
            partition by teacher_user_id, workshop_organizer, workshop_course
            order by form_submitted_at desc) as rnk 
    from {{ ref('stg_dashboard_pii__pd_international_opt_ins')}}
),

international_partners as (
    select *
    from {{ ref('stg_legacy__international_partners') }}
),

combined as (
    select 
        -- partner info 
        partner_id,
        partner_name,
        partner_type,

        -- regional info 
        country_code,
        country_name,
        alt_country_name, -- decided what to do with this later (js)

        -- teacher info
        teacher_user_id,
        form_submitted_at as last_opt_in_at,
        updated_at as last_opt_in_updated_at,
        lower(first_name)   as first_name,
        lower(pref_name)    as preferred_name,
        lower(last_name)    as last_name,
        lower(email)        as email,
        lower(email_alt)    as email_alt,
        gender,

        -- school info
        school_department,
        school_municipality,
        school_name,
        school_city,
        school_country,
        age_taught,
        subject_taught,
        cs_resources,
        robotics_resources, -- possibly deprecated? (js)

        -- workshop info (I could see this as a separate model -js)
        workshop_organizers,    -- regional_partner?
        workshop_facilitator,
        workshop_course,
        contact_name,
        contact_email,
        workshop_date
    from opt_ins
    left join international_partners 
        on lower(opt_ins.workshop_organizer) = international_partners.workshop_organizers
    where opt_ins.rnk = 1
)

select *
from combined 