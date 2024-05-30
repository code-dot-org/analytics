/* 

    Macro:  unpivot_big_table(table_name_ref, num_fixed_columns)

    Description:    This macro dynamically generates SQL for a redshift UNPIVOT command. 
                    Especially useful for cases where you need to reshape a table from wide to long, 
                    and you have a VERY wide table (like AP exam results' 89 columns).  Redshift's 
                    UNPIVOT command requires you to list out all the columns by name, and this can be 
                    tedious.  This macro essentially just lists out all the column names as strings 
                    dynamically for you.

                    A previous version of this operation used Jinja to reshape the table with nested 
                    loops to create short little tables into a giant UNION operation. Here we use 
                    redshift's UNPIVOT with the assumption that it will be more performant and easier 
                    to read.

    Returns:    an SQL SELECT statement to UNPIVOT data from a specified table with a specified number 
                of fixed columns. The result of running the SQL will turn all columns from the UNPIVOT 
                into pairs of 'orig_col_name','orig_value'

    param:  table_name_ref - a string of the table to be referenced, the code uses dbt's: ref(tablename)
    param:  num_fixed_columns - an integer of the n left-most columns in the table you want to stay fixed 
            for data to unpivot around. The remaining columns will all be turned into key-value pairs with 
            column names 'orig_col_name' and 'orig_value'
*/



{% macro unpivot_big_table(table_name_ref, num_fixed_columns) %}

    -- Get a list of column names from the table, and split into two lists of (1) the
    {% set my_table_columns = adapter.get_columns_in_relation(ref(table_name_ref)) %}

    -- for the fixed columns, construct a string of "comma", "separated", "lists","of", "double-quoted","col", "names"
    {% set fixed_cols_str = (
        '"' ~ my_table_columns[:num_fixed_columns]
        | map(attribute="name")
        | join('",\n"') ~ '"'
    ) %}

    -- for the set of column to unpivot, construct a string of "comma", "separated", "lists", "of", "double-quoted","col", "names"
    {% set pivot_cols_str = (
        '"' ~ my_table_columns[num_fixed_columns:]
        | map(attribute="name")
        | join('",\n"') ~ '"'
    ) %}

    -- Construct the SELECT statament for the UNPIVOT
    select 
        {{ fixed_cols_str }}, 
        orig_col_name, 
        orig_value
    from(
        select 
            {{ fixed_cols_str }}, 
            {{ pivot_cols_str }} 
        from {{ ref(table_name_ref) }}
    ) as sourcetable
    unpivot (orig_value for orig_col_name in ({{ pivot_cols_str }}))

{% endmacro %}
