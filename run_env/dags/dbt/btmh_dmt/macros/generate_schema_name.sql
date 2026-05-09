{#
  Custom schema naming to support Airflow runs where the profile dataset (target.schema)
  may not exist (or may be in a different region), while still writing models into the
  existing fixed datasets like `dwh_fact` and `dwh_dim`.

  This macro is NO-OP by default. It only changes behavior when
  `var('btmh_use_fixed_datasets', false)` is true.

  When enabled:
    - custom schema 'fact' -> '<prefix>_fact'
    - custom schema 'dim'  -> '<prefix>_dim'
    - custom schema 'mart' -> '<prefix>_mart'

  Where <prefix> is `var('btmh_fixed_prefix', 'dwh')`.
#}

{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {%- set fixed = var('btmh_use_fixed_datasets', false) -%}

  {%- if fixed and custom_schema_name is not none -%}
    {%- set cs = (custom_schema_name | string | trim) -%}
    {%- set prefix = (var('btmh_fixed_prefix', 'dwh') | string | trim) -%}

    {%- if cs in ['fact', 'dim', 'mart'] -%}
      {{ return(prefix ~ '_' ~ cs) }}
    {%- else -%}
      {{ return(cs) }}
    {%- endif -%}

  {%- else -%}
    {# Default dbt behavior (equivalent to dbt's default__generate_schema_name) #}
    {%- set default_schema = (target.schema | string | trim) -%}

    {#
      Extra safety:
      If the profile dataset is already a derived dataset (e.g. dwh_fact) and models use
      +schema: fact|dim|mart, the vanilla default would produce dwh_fact_fact which is wrong.
      In that case, treat target.schema as the *base* for deriving the fixed datasets.
    #}
    {%- if custom_schema_name is not none -%}
      {%- set cs = (custom_schema_name | string | trim) -%}
      {%- if cs in ['fact', 'dim', 'mart'] -%}
        {%- set prefix = default_schema -%}
        {%- if prefix.endswith('_fact') -%}
          {%- set prefix = prefix[: -5] -%}
        {%- elif prefix.endswith('_dim') -%}
          {%- set prefix = prefix[: -4] -%}
        {%- elif prefix.endswith('_mart') -%}
          {%- set prefix = prefix[: -5] -%}
        {%- endif -%}
        {%- if prefix and prefix != default_schema -%}
          {{ return(prefix ~ '_' ~ cs) }}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}

    {%- if custom_schema_name is none -%}
      {{ return(default_schema) }}
    {%- else -%}
      {{ return(default_schema ~ '_' ~ (custom_schema_name | string | trim)) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
