{%- macro drop_vestigial_relations(
  exclude_schemas=[],
  dry_run=False,
  raise_on_dry_run=False
  ) %}
  {%- if exclude_schemas is string %}
    {%- set exclude_schemas = [exclude_schemas] %}
  {%- elif exclude_schemas is not iterable %}
    {%- do exceptions.raise_compiler_error('`exclude_schemas` must be a string or a list') %}
  {%- endif %}

  {#- Always exclude the test audit schema  #}
  {%- set exclude_schemas = exclude_schemas + [target.schema ~ '_dbt_test__audit'] %}

  {#- Identify non-dbt tables in dbt-referenced schemas, if any, as '<schemaname>.<tablename>' #}
  {%- set non_dbt_tables = [] %}

  {%- if execute %}
    {%- set nodes_to_check =
      graph.nodes.values() | selectattr('resource_type', 'in', ['model', 'seed', 'snapshot']) | list %}
    {%- set graph_schemas = [] %}
    {%- for node in graph.nodes.values() %}
      {%- if node.schema not in graph_schemas and node.schema not in exclude_schemas %}
        {%- do graph_schemas.append(node.schema) %}
      {%- endif %}
    {%- endfor %}

  {%- do log('Finding vestigial tables in these schemas:\n\t\t' ~ graph_schemas|join('\n\t\t'), info=True) %}

    {%- call statement('get_vestigial_tables', fetch_result=True) %}
    with current_graph as (

      {%- for node in nodes_to_check %}
      select '{{ node.schema }}' as schema_name, '{{ node.name }}' as ref_name
      {%- if not loop.last %}
      union all{% endif %}
      {%- endfor %}

    ),

    current_tables as (

      select
        schemaname as schema_name,
        tablename as ref_name,
        'table' as ref_type
      from pg_catalog.pg_tables
      where schemaname in (
        {% for s in graph_schemas %}'{{ s }}'{% if not loop.last %},{% endif %}
        {% endfor -%})

    ),

    current_views as (

      select
        schemaname as schema_name,
        viewname as ref_name,
        'view' as ref_type
      from pg_catalog.pg_views
      where schemaname in (
        {% for s in graph_schemas %}'{{ s }}'{% if not loop.last %},{% endif %}
        {% endfor -%})

    ),

    current_db as (

      select * from current_tables
      union all select * from current_views

    )

    select current_db.*
    from
      current_db
      left join current_graph
          on  current_graph.schema_name = current_db.schema_name
          and current_graph.ref_name = current_db.ref_name
    where current_graph.ref_name is null
    order by current_db.schema_name, current_db.ref_name
    {%- endcall %}

    {%- set full_results = load_result('get_vestigial_tables')['data'] %}
    {%- set to_delete = [] %}
    {% for item in full_results %}
      {%- set item_name = item[0] ~ '.' ~ item[1]%}
      {%- if item_name not in non_dbt_tables %}
        {%- do to_delete.append(item)%}
      {%- else %}
        {%- do log('Ignoring non-dbt relation "' ~ item_name ~ '"', info=true)%}
      {%- endif %}
    {% endfor %}

    {%- if to_delete %}
      {% set drop_query_list = ['begin;'] %}
      {%- for item in to_delete %}
        {%- set drop_statement %}  drop {{ item[2] }} if exists "{{ item[0] }}"."{{ item[1] }}" cascade;{%- endset %}
        {%- do drop_query_list.append(drop_statement) %}
      {%- endfor %}

      {%- do drop_query_list.append('commit;') %}
      {%- set drop_query = drop_query_list|join('\n') %}
      
      {%- if not dry_run %}
        {%- do log('Executing the following statements:', info=true) %}
        {%- do log(drop_query, info=true) %}
        {%- do run_query(drop_query) %}
      {%- else %}
        {%- do log("", info=true) %}
        {%- do log('DRY RUN produced the following statements:\n\t\t' ~ drop_query_list|join('\n\t\t'), info=true) %}
        {% if raise_on_dry_run %}
          {%- set err_msg -%}
            {{ to_delete|length }} vestigial tables found during dry run. See logs for details.
          {%- endset %}
          {%- do exceptions.raise_compiler_error(err_msg) %}
        {% endif %}
      {%- endif %}
    {%- else %}
      {%- do log('No vestigial tables found', info=true) %}
    {%- endif %}
  {%- endif %}
{%- endmacro %}