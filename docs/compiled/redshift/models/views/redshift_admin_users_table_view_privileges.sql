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

), objects as (
  
  select
    schema_name
  , 'table' as object_type
  , table_name as object_name
  , '"' || schema_name || '"."' || table_name || '"' as full_object_name
  from tables
  where schema_name not in ('pg_catalog', 'information_schema')
  
  union
  
  select
    schema_name
  , 'view' as object_type
  , view_name as object_name
  , '"' || schema_name || '"."' || view_name || '"' as full_object_name
  from views
  where schema_name not in ('pg_catalog', 'information_schema')
  
)

select 
  objects.schema_name
, objects.object_name
, users.username
, has_table_privilege(users.username, objects.full_object_name, 'select') as has_select_privilege
, has_table_privilege(users.username, objects.full_object_name, 'insert') as has_insert_privilege
, has_table_privilege(users.username, objects.full_object_name, 'update') as has_update_privilege
, has_table_privilege(users.username, objects.full_object_name, 'delete') as has_delete_privilege
, has_table_privilege(users.username, objects.full_object_name, 'references') as has_references_privilege
from objects
cross join users
order by objects.full_object_name, users.username