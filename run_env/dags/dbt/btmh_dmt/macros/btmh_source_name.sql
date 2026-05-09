{% macro btmh_source_name(s) %}
  {% if s is mapping and s.get('source') %}
    {{ return(s.get('source')) }}
  {% elif s is mapping and s.get('schema') %}
    {{ return((s.get('schema') | lower)) }}
  {% else %}
    {% do exceptions.raise_compiler_error("Invalid source config: expected mapping with key 'source' or 'schema'") %}
  {% endif %}
{% endmacro %}
