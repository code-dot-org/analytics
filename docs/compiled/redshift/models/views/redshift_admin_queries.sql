with  __dbt__cte__stl_query as (
select

  userid as user_id
, query as query_id
, xid as transaction_id
, label
, pid
, database
, starttime as started_at
, endtime as finished_at
, aborted

from pg_catalog.stl_query
),  __dbt__cte__pg_user as (
select

  usesysid as user_id
, usename as username

from pg_catalog.pg_user
),  __dbt__cte__stl_explain as (
select

  userid as user_id
, query as query_id
, nodeid
, parentid
, plannode
, info

from pg_catalog.stl_explain
),  __dbt__cte__redshift_cost as (
with stl_explain as (

  select query_id, plannode from __dbt__cte__stl_explain
  where nodeid = 1

), parse_step_one as (

  -- plannode (which contains cost) is formatted like:
  --   XN Seq Scan on nyc_last_update  (cost=0.00..0.03 rows=2 width=40)
  -- we want to rip out the cost part (0.00, 0.03) and make it usable.
  -- cost_string after this step is "0.00..0.03 ..."
  select
    query_id
  , split_part(plannode, 'cost=', 2) as cost_string

  from stl_explain

), parse_step_two as (

  select
    query_id
  , split_part(cost_string, '..', 1) as starting_cost
  , substring(
      split_part(cost_string, '..', 2)
      from 1
      for strpos(split_part(cost_string, '..', 2), ' ')) as total_cost

  from parse_step_one

)


select

  query_id
, starting_cost::float as starting_cost
, total_cost::float as total_cost

from parse_step_two
),  __dbt__cte__stl_wlm_query as (
select

  userid as user_id
, query as query_id
, xid
, task
, service_class
, slot_count
, service_class_start_time
, queue_start_time
, queue_end_time
, total_queue_time
, exec_start_time
, exec_end_time
, total_exec_time
, service_class_end_time
, final_state

from pg_catalog.stl_wlm_query
), queries as (

  select * from __dbt__cte__stl_query

), users as (

  select * from __dbt__cte__pg_user

), cost as (

  select * from __dbt__cte__redshift_cost

), timings as (

  select * from __dbt__cte__stl_wlm_query

)



select

  queries.query_id
, queries.transaction_id
, users.username::varchar

, cost.starting_cost
, cost.total_cost

, queries.started_at
, queries.finished_at

, timings.queue_start_time
, timings.queue_end_time
, (timings.total_queue_time::float / 1000000.0) as total_queue_time_seconds

, timings.exec_start_time
, timings.exec_end_time
, (timings.total_exec_time::float / 1000000.0) as total_exec_time_seconds

from queries

left join users
  on queries.user_id = users.user_id

left join cost
  on queries.query_id = cost.query_id

left join timings
  on queries.query_id = timings.query_id