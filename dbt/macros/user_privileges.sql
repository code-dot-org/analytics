{# first time writing one of these, will probably not deploy but
  saving code for future reference #}

-- schema names to iterate through
{% set schema_name = [
  "dashboard_production", 
  "dashboard_production_pii", 
  "analysis",
  "analytics",
  "dbt_%"
] %},

-- access to update
{% set user_group = [
  "reader_pii_dbt", 
  "writer_pii_dbt"
] %}

{# apply read access for dashboard schemas #}
  {% for schema_name in schema_names %}
  case when schema_name in ('dashboard_production',
    'dashboard_production_pii',
    'analysis')
  grant usage 
    on schema '{{schema_name}}' 
      to group reader_pii_dbt

  grant select 
    on all tables 
    in schema '{{schema_name}}'   
      to group reader_pii_dbt

  alter default privileges 
    for user dbt 
    in schema '{{schema_name}}' 
    grant select 
      on tables 
      to group reader_pii_dbt

 {# apply write access for dbt schemas #}
case when schema_name = 'analytics'
  or left(schema_name,3) = 'dbt'

  grant usage 
    on schema '{{schema_name}}' 
      to group writer_pii_dbt

  grant select 
    on all tables 
    in schema '{{schema_name}}'   
      to group writer_pii_dbt

  alter default privileges 
    for user dbt 
    in schema '{{schema_name}}' 
    grant select 
      on tables 
      to group writer_pii_dbt
  {% endfor %}
