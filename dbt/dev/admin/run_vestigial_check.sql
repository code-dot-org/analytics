-- run macro drop_vestigial_relations
{{ config(
    tags=["exclude_from_production"]
) }}

{{ drop_vestigial_relations(dry_run=true) }}