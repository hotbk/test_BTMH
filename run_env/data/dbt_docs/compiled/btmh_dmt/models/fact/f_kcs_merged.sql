

with src as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`{{ env_var('ggs_bq_dataset', 'btmh_stg_ggs') }}`.`ggs_kcs`
),

dedup as (
  select
    s.*
  from src s
  qualify row_number() over (
    partition by ma_hang
    order by
      coalesce(
        
(
  case
    when ngay_gio_xac_nhan is null then null

    -- epoch-like numerics
    when regexp_contains(cast(ngay_gio_xac_nhan as string), r'^\d{19}$') then timestamp_micros(div(cast(ngay_gio_xac_nhan as int64), 1000))
    when regexp_contains(cast(ngay_gio_xac_nhan as string), r'^\d{16}$') then timestamp_micros(cast(ngay_gio_xac_nhan as int64))
    when regexp_contains(cast(ngay_gio_xac_nhan as string), r'^\d{13}$') then timestamp_millis(cast(ngay_gio_xac_nhan as int64))
    when regexp_contains(cast(ngay_gio_xac_nhan as string), r'^\d{10}$') then timestamp_seconds(cast(ngay_gio_xac_nhan as int64))

    -- yyyymmddhhmmss as int/string
    when regexp_contains(cast(ngay_gio_xac_nhan as string), r'^\d{14}$') then timestamp(
      parse_datetime('%Y%m%d%H%M%S', cast(ngay_gio_xac_nhan as string)),
      "Asia/Ho_Chi_Minh"
    )

    -- yyyymmdd as int/string
    when regexp_contains(cast(ngay_gio_xac_nhan as string), r'^\d{8}$') then timestamp(
      datetime(parse_date('%Y%m%d', cast(ngay_gio_xac_nhan as string))),
      "Asia/Ho_Chi_Minh"
    )

    -- Fall back to BigQuery's parser (handles ISO strings); timezone only applies if the string has no zone.
    else timestamp(cast(ngay_gio_xac_nhan as string), "Asia/Ho_Chi_Minh")
  end
)
,
        
(
  case
    when ngay_lam_phieu is null then null

    -- epoch-like numerics
    when regexp_contains(cast(ngay_lam_phieu as string), r'^\d{19}$') then timestamp_micros(div(cast(ngay_lam_phieu as int64), 1000))
    when regexp_contains(cast(ngay_lam_phieu as string), r'^\d{16}$') then timestamp_micros(cast(ngay_lam_phieu as int64))
    when regexp_contains(cast(ngay_lam_phieu as string), r'^\d{13}$') then timestamp_millis(cast(ngay_lam_phieu as int64))
    when regexp_contains(cast(ngay_lam_phieu as string), r'^\d{10}$') then timestamp_seconds(cast(ngay_lam_phieu as int64))

    -- yyyymmddhhmmss as int/string
    when regexp_contains(cast(ngay_lam_phieu as string), r'^\d{14}$') then timestamp(
      parse_datetime('%Y%m%d%H%M%S', cast(ngay_lam_phieu as string)),
      "Asia/Ho_Chi_Minh"
    )

    -- yyyymmdd as int/string
    when regexp_contains(cast(ngay_lam_phieu as string), r'^\d{8}$') then timestamp(
      datetime(parse_date('%Y%m%d', cast(ngay_lam_phieu as string))),
      "Asia/Ho_Chi_Minh"
    )

    -- Fall back to BigQuery's parser (handles ISO strings); timezone only applies if the string has no zone.
    else timestamp(cast(ngay_lam_phieu as string), "Asia/Ho_Chi_Minh")
  end
)
,
        timestamp(datetime(dt), 'Asia/Ho_Chi_Minh')
      ) desc
  ) = 1
)

select *
from dedup