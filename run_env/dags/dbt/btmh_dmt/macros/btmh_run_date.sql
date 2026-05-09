{% macro btmh_run_date() %}
(
  {% if var('run_date', none) %}
    date({{ var('run_date') | tojson }})
  {% else %}
    current_date()
  {% endif %}
)
{% endmacro %}


{% macro btmh_start_date(months_back=2) %}
(
  {% if var('btmh_today_only', false) %}
    {{ btmh_run_date() }}
  {% else %}
    date_sub({{ btmh_run_date() }}, interval {{ months_back }} month)
  {% endif %}
)
{% endmacro %}



{% macro btmh_window_start_date(months_back=2) %}
(
  {% if var('snapshot_start_date', none) %}
    date({{ var('snapshot_start_date') | tojson }})
  {% else %}
    {{ btmh_start_date(months_back) }}
  {% endif %}
)
{% endmacro %}


{% macro btmh_window_end_date() %}
(
  {% if var('snapshot_end_date', none) %}
    date({{ var('snapshot_end_date') | tojson }})
  {% else %}
    {{ btmh_run_date() }}
  {% endif %}
)
{% endmacro %}


{% macro btmh_is_backfill() %}
  {#
    Airflow/Cosmos often passes missing date params as empty strings ("") rather than null.
    Treat empty strings as "not provided" to avoid accidentally forcing backfill mode.
  #}

  {% set snapshot_start = (var('snapshot_start_date', '') | string | trim) %}
  {% set snapshot_end = (var('snapshot_end_date', '') | string | trim) %}

  {{ return(
    var('btmh_is_backfill', false)
    or var('btmh_snapshot_append', false)
    or snapshot_start != ''
    or snapshot_end != ''
  ) }}
{% endmacro %}
