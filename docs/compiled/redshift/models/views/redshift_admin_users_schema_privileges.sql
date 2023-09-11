with  __dbt__cte__pg_tables as (
select
  schemaname as schema_name
, tablename as table_name
, tableowner as table_owner
, tablespace as table_space
, hasindexes as has_indexes
, hasrules as has_rules
, hastriggers as has_triggers
from pg_catalog.pg_tables
),  __dbt__cte__pg_views as (
select
  schemaname as schema_name
, viewname as view_name
, viewowner as view_owner
from pg_catalog.pg_views
),  __dbt__cte__pg_user as (
select

  usesysid as user_id
, usename as username

from pg_catalog.pg_user
), tables as (

  select * from __dbt__cte__pg_tables

), views as (

  select * from __dbt__cte__pg_views

), users as (

  select * from __dbt__cte__pg_user
  
), schemas as (
  
  select
  distinct(schema_name)
  from tables
  where schema_name not in ('pg_catalog', 'information_schema')
        
  union
        
  select
  distinct(schema_name)
  from views
        
  where schema_name not in ('pg_catalog', 'information_schema')
  
)


select 
  schemas.schema_name
, users.username
, has_schema_privilege(users.username, schemas.schema_name, 'usage') AS has_usage_privilege
, has_schema_privilege(users.username, schemas.schema_name, 'create') AS has_create_privilege
from schemas
cross join users
order by schemas.schema_name, users.username