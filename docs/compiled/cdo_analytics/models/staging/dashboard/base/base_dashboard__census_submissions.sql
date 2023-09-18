with 
source as (
      select * from "dashboard"."dashboard_production"."census_submissions"
),

renamed as (
    select
        id                                      as census_submissions_id,
        type                                    as census_submissions_type,
        submitter_role,
        school_year,
        how_many_do_hoc,
        how_many_after_school,
        how_many_10_hours,
        how_many_20_hours,
        other_classes_under_20_hours,
        topic_blocks                            as is_topic_blocks,
        topic_text                              as is_topic_text,
        topic_robots                            as is_topic_robots,
        topic_internet                          as is_topic_internet,
        topic_security                          as is_topic_security,
        topic_data                              as is_topic_data,
        topic_web_design                        as is_topic_web_design,
        topic_game_design                       as is_topic_game_design,
        topic_other                             as is_topic_other,
        topic_other_description,
        topic_do_not_know                       as is_topic_do_not_know,
        class_frequency,
        pledged                                 as is_pledged,
        created_at,
        updated_at,
        share_with_regional_partners            as is_shared_with_regional_partners,
        topic_ethical_social                    as is_topic_ethical_social,
        inaccuracy_reported                     as is_inaccuracy_reported
    from source
)

select * from renamed