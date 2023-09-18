

with __dbt__cte__pg_class as (
select
  oid
, relname
, relnamespace
, reltype
, relowner
, relam
, relfilenode
, reltablespace
, relpages
, reltuples
, reltoastrelid
, reltoastidxid
, relhasindex
, relisshared
, relkind
, relnatts
, relexternid
, relisreplicated
, relispinned
, reldiststyle
, relprojbaseid
, relchecks
, reltriggers
, relukeys
, relfkeys
, relrefs
, relhasoids
, relhaspkey
, relhasrules
, relhassubclass
, relacl
from pg_catalog.pg_class
),  __dbt__cte__pg_depend as (
select
  classid
, objid
, objsubid
, refclassid
, refobjid
, refobjsubid
, deptype
from pg_catalog.pg_depend
),  __dbt__cte__pg_namespace as (
select
  oid
, nspname
, nspowner
, nspacl
from pg_catalog.pg_namespace
) select distinct
  srcobj.oid as source_oid
  , srcnsp.nspname as source_schemaname
  , srcobj.relname as source_objectname
  , tgtobj.oid as dependent_oid
  , tgtnsp.nspname as dependent_schemaname
  , tgtobj.relname as dependent_objectname

from

  __dbt__cte__pg_class as srcobj
  join __dbt__cte__pg_depend as srcdep on srcobj.oid = srcdep.refobjid
  join __dbt__cte__pg_depend as tgtdep on srcdep.objid = tgtdep.objid
  join __dbt__cte__pg_class as tgtobj
    on tgtdep.refobjid = tgtobj.oid
    and srcobj.oid <> tgtobj.oid
  left join __dbt__cte__pg_namespace as srcnsp
    on srcobj.relnamespace = srcnsp.oid
  left join __dbt__cte__pg_namespace tgtnsp on tgtobj.relnamespace = tgtnsp.oid

where
  tgtdep.deptype = 'i' --dependency_internal
  and tgtobj.relkind = 'v' --i=index, v=view, s=sequence