-- macros/clean_phase.sql
-- Normalises messy phase strings to a standard controlled vocabulary.

{% macro clean_phase(column_name) %}
    case
        when lower(trim({{ column_name }})) like '%early phase 1%'    then 'early phase 1'
        when lower(trim({{ column_name }})) like '%phase 1/phase 2%'  then 'phase 1/phase 2'
        when lower(trim({{ column_name }})) like '%phase 2/phase 3%'  then 'phase 2/phase 3'
        when lower(trim({{ column_name }})) like '%phase 1%'          then 'phase 1'
        when lower(trim({{ column_name }})) like '%phase 2%'          then 'phase 2'
        when lower(trim({{ column_name }})) like '%phase 3%'          then 'phase 3'
        when lower(trim({{ column_name }})) like '%phase 4%'          then 'phase 4'
        when {{ column_name }} is null or trim({{ column_name }}) = '' then 'n/a'
        else 'n/a'
    end
{% endmacro %}
