{#
   name: run_proc__rosetta
   usage: dbt run-operation run_proc__rosetta
   changelog:
    - v1 (js) 2024-06-29
#}

{% macro run_rosetta() %}
    CALL analysis.run_rosetta();
    
{% endmacro %}
