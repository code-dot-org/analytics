with  __dbt__cte__stv_tbl_perm as (
select
  slice
, id -- table id
, name -- table name
, rows
, sorted_rows
, (rows - sorted_rows) as unsorted_rows
, temp
, db_id
, backup
from pg_catalog.stv_tbl_perm
),  __dbt__cte__pg_class as (
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
),  __dbt__cte__pg_namespace as (
select
  oid
, nspname
, nspowner
, nspacl
from pg_catalog.pg_namespace
),  __dbt__cte__stv_blocklist as (
select
  slice
, col
, tbl
, blocknum
, num_values
, extended_limits
, minvalue
, maxvalue
, sb_pos
, pinned
, on_disk
, backed_up
, modified
, hdr_modified
, unsorted
, tombstone
, preferred_diskno
, temporary
, newblock
, num_readers
, id
, flags
from pg_catalog.stv_blocklist
),  __dbt__cte__pg_attribute as (
select
  attrelid
, attname
, atttypid
, attstattarget
, attlen
, attnum
, attndims
, attcacheoff
, atttypmod
, attbyval
, attstorage
, attalign
, attnotnull
, atthasdef
, attisdropped
, attislocal
, attinhcount
, attisdistkey
, attispreloaded
, attsortkeyord
, attencodingtype
, attencrypttype
, (case attisdistkey
        when 't' then attname
        else null end) as dist_key
, (case attsortkeyord
        when 1 then attname
        else null end) as sort_key
from pg_catalog.pg_attribute
),  __dbt__cte__svv_diskusage as (
select
  db_id
, name
, slice
, col
, tbl
, blocknum
, num_values
, extended_limits
, minvalue
, maxvalue
, sb_pos
, pinned
, on_disk
, backed_up
, modified
, hdr_modified
, unsorted
, tombstone
, preferred_diskno
, temporary
, newblock
, num_readers
, id
, flags
from pg_catalog.svv_diskusage
),  __dbt__cte__stv_partitions as (
select

  owner
, host
, diskno
, part_begin
, part_end
, used
, tossed
, capacity
, "reads"
, writes
, seek_forward
, seek_back
, is_san
, failed
, mbps
, mount

from pg_catalog.stv_partitions
), unsorted_by_table as (

  select
    db_id
  , id as table_id
  , name as table_name
  , sum(rows) as rows
  , sum(unsorted_rows) as unsorted_rows
  from __dbt__cte__stv_tbl_perm
  group by 1, 2, 3

), pg_class as (

  select * from __dbt__cte__pg_class

), pg_namespace as (

  select * from __dbt__cte__pg_namespace

), table_sizes as (

  select
    tbl as table_id
  , count(*) as size_in_megabytes
  from __dbt__cte__stv_blocklist
  group by 1

), table_attributes as (

  select
    attrelid as table_id
  , min(dist_key) as dist_key
  , min(sort_key) as sort_key
  , max(attsortkeyord) as num_sort_keys
  , (max(attencodingtype) > 0) as is_encoded
  , max(attnum) as num_columns
  from __dbt__cte__pg_attribute
  group by 1

), slice_distribution as (

  select
    tbl as table_id
  , trim(name) as name
  , slice
  , count(*) as size_in_megabytes

  from __dbt__cte__svv_diskusage
  group by 1, 2, 3

), capacity as (

  select
    sum(capacity) as total_megabytes
  from __dbt__cte__stv_partitions
  where part_begin=0

), table_distribution_ratio as (

  select
    table_id
  , (max(size_in_megabytes)::float / min(size_in_megabytes)::float)
      as ratio
  from slice_distribution
  group by 1

)

select

  trim(pg_namespace.nspname) as schema
, trim(unsorted_by_table.table_name) as table
, unsorted_by_table.rows
, unsorted_by_table.unsorted_rows
, (case unsorted_by_table.rows
        when 0 then 0
        else (unsorted_by_table.unsorted_rows::float / unsorted_by_table.rows::float) * 100.0 end)
    as percent_rows_unsorted
, unsorted_by_table.table_id

, decode(pg_class.reldiststyle, 0, 'even',
                              1, table_attributes.dist_key,
                              'all') as dist_style
, table_distribution_ratio.ratio as dist_skew

, (table_attributes.sort_key is not null) as is_sorted
, table_attributes.sort_key
, table_attributes.num_sort_keys
, table_attributes.num_columns

, table_sizes.size_in_megabytes
, (case capacity.total_megabytes
        when 0 then 0
        else (table_sizes.size_in_megabytes::float / capacity.total_megabytes::float) * 100.0 end)
    as disk_used_percent_of_total
, table_attributes.is_encoded

from unsorted_by_table

inner join pg_class
  on pg_class.oid = unsorted_by_table.table_id

inner join pg_namespace
  on pg_namespace.oid = pg_class.relnamespace

inner join capacity
  on 1=1

left join table_sizes
  on unsorted_by_table.table_id = table_sizes.table_id

inner join table_attributes
  on table_attributes.table_id = unsorted_by_table.table_id

inner join table_distribution_ratio
  on table_distribution_ratio.table_id = unsorted_by_table.table_id