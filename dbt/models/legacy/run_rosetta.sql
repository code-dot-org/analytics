{#
   name: run_proc__rosetta
   usage: dbt run-operation run_proc__rosetta
   changelog:
    - v1 (js) 2024-06-29
#}

-- {% macro run_rosetta(run_rosetta) %}
--     CALL 
--         analysis.run_rosetta()
    
-- {% endmacro %}

{% macro run_stored_procedure(run_rosetta) %}
  {% set sql %}
    EXECUTE {{ analysis.run_rosetta }}
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}