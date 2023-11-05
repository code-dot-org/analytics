with {% snapshot dim_user_levels__snapshot %}

{{
    config(
      target_database='dev',
      target_schema='dbt_jordan',   {# 'analytics '#}
      unique_key='user_level_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('dbt_jordan', 'dim_user_levels') }}

{% endsnapshot %}
