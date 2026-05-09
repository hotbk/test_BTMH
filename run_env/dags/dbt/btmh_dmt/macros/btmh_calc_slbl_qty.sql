{% macro btmh_calc_slbl_qty(col_so_luong, col_ma_nhom, col_ma_hang, col_t_luong, col_the_tich, col_tien_lai, col_tyle_lai) %}
(
  case
    when (
      {{ col_ma_nhom }} in ('NL24', 'NLTT')
      or substr({{ col_ma_nhom }}, 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or {{ col_ma_hang }} in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr({{ col_ma_nhom }}, 1, 3) in ('KGB', 'KHS') then ({{ col_so_luong }} * {{ col_t_luong }})
        when substr({{ col_ma_nhom }}, 1, 3) in ('NTQ', 'NGB') then ({{ col_so_luong }} * {{ col_t_luong }})
        when substr({{ col_ma_hang }}, 1, 4) in ('TTSJ', 'TTVR') then ({{ col_so_luong }} * {{ col_t_luong }})
        when substr({{ col_ma_hang }}, 1, 2) in ('VD', '24', 'VT') then ({{ col_so_luong }} * {{ col_t_luong }})
        when {{ col_ma_hang }} in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then ({{ col_so_luong }} * {{ col_t_luong }})
        else (
          {{ col_so_luong }} * (
            {{ col_t_luong }}
            + {{ col_the_tich }}
            + {{ col_tien_lai }}
            + {{ col_tyle_lai }}
          )
        )
      end
    )
    else {{ col_so_luong }}
  end
)
{% endmacro %}
