with 
source as (
      select * from "dashboard"."dashboard_production_pii"."peer_reviews"
),

renamed as (
    select
        id as peer_review_id,
        submitter_id,
        reviewer_id,
        from_instructor,
        script_id,
        level_id,
        level_source_id,
        data,
        status,
        created_at,
        updated_at,
        audit_trail
    from source
)

select * from renamed