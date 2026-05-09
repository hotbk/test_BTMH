{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date"},
  )
}}

with params as (
  select
    {{ btmh_run_date() }} as run_date,
    {{ btmh_start_date(2) }} as start_date
),

ty_gia as (
    select ngay, Gia_Mua_Trung_Binh
    from {{ ref('f_ty_gia') }}
    where ngay between (select start_date from params) and (select run_date from params)
      and Ma_VBTG = 'NL9999'
),

base as (
    select  x.ngay,
            x.gia_dong as xau,
            v.gia_dong as usd,
            (x.gia_dong * v.gia_dong) / 0.82945 / 10 as xaud_quy_doi,
            t.Gia_Mua_Trung_Binh,
            t.Gia_Mua_Trung_Binh 
                - (x.gia_dong * v.gia_dong) / 0.82945 / 10 as dp
    from {{ source('crawl_data', 'xau_usd') }} x
    inner join {{ source('crawl_data', 'vnd_usd') }} v using (ngay)
    inner join ty_gia t using (ngay)
)

select
    b1.ngay,
    b1.xau,
    b1.usd,
    b1.xaud_quy_doi,
    b1.Gia_Mua_Trung_Binh,
    b1.dp,

    -- Percentile toàn thời gian
    percent_rank() over (order by b1.dp) 
        as percentile_all_time,

    -- Rolling 12 months percentile
    safe_divide(
        countif(b2.dp <= b1.dp),
        count(b2.dp)
    ) as percentile_12m

from base b1
join base b2
    on b2.ngay between date_sub(b1.ngay, interval 12 month)
                   and b1.ngay

{% if is_incremental() %}
where b1.ngay >= (select start_date from params)
  and b1.ngay <= (select run_date from params)
{% endif %}

group by
    b1.ngay,
    b1.xau,
    b1.usd,
    b1.xaud_quy_doi,
    b1.Gia_Mua_Trung_Binh,
    b1.dp

