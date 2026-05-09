{% macro btmh_to_timestamp_any(value, tz='Asia/Ho_Chi_Minh') %}
(
  case
    when {{ value }} is null then null

    -- epoch-like numerics
    when regexp_contains(cast({{ value }} as string), r'^\d{19}$') then timestamp_micros(div(cast({{ value }} as int64), 1000))
    when regexp_contains(cast({{ value }} as string), r'^\d{16}$') then timestamp_micros(cast({{ value }} as int64))
    when regexp_contains(cast({{ value }} as string), r'^\d{13}$') then timestamp_millis(cast({{ value }} as int64))
    when regexp_contains(cast({{ value }} as string), r'^\d{10}$') then timestamp_seconds(cast({{ value }} as int64))

    -- yyyymmddhhmmss as int/string
    when regexp_contains(cast({{ value }} as string), r'^\d{14}$') then timestamp(
      parse_datetime('%Y%m%d%H%M%S', cast({{ value }} as string)),
      {{ tz | tojson }}
    )

    -- yyyymmdd as int/string
    when regexp_contains(cast({{ value }} as string), r'^\d{8}$') then timestamp(
      datetime(parse_date('%Y%m%d', cast({{ value }} as string))),
      {{ tz | tojson }}
    )

    -- Fall back to BigQuery's parser (handles ISO strings); timezone only applies if the string has no zone.
    else timestamp(cast({{ value }} as string), {{ tz | tojson }})
  end
)
{% endmacro %}
