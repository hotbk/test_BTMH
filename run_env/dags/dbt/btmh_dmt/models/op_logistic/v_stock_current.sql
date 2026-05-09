select *
from {{ ref('v_stock_history') }}
where Ngay = {{ btmh_run_date() }}
