

with params as (
  select
    
(
  
    current_date()
  
)
 as run_date,
    extract(year from 
(
  
    current_date()
  
)
) as curr_year,
    extract(month from 
(
  
    current_date()
  
)
) as curr_month,
    extract(year from date_sub(
(
  
    current_date()
  
)
, interval 1 month)) as prev_year,
    extract(month from date_sub(
(
  
    current_date()
  
)
, interval 1 month)) as prev_month
),

src as (
  select
    *
  from `btmh-airflow-dbt-lab-2026`.`{{ env_var('ggs_bq_dataset', 'btmh_stg_ggs') }}`.`ggs_mkt`
  qualify dt = max(dt) over()
),

filtered as (
  select
    safe_cast(nam as int64) as nam,
    safe_cast(thang as int64) as thang,
    cast(kenh as string) as kenh,
    cast(chi_tieu as string) as chi_tieu,
    cast(don_vi as string) as don_vi,
    cast(danh_muc_san_pham as string) as danh_muc_san_pham,
    cast(dong_san_pham as string) as dong_san_pham,

    safe_cast(ngay_1 as float64) as ngay_1,
    safe_cast(ngay_2 as float64) as ngay_2,
    safe_cast(ngay_3 as float64) as ngay_3,
    safe_cast(ngay_4 as float64) as ngay_4,
    safe_cast(ngay_5 as float64) as ngay_5,
    safe_cast(ngay_6 as float64) as ngay_6,
    safe_cast(ngay_7 as float64) as ngay_7,
    safe_cast(ngay_8 as float64) as ngay_8,
    safe_cast(ngay_9 as float64) as ngay_9,
    safe_cast(ngay_10 as float64) as ngay_10,
    safe_cast(ngay_11 as float64) as ngay_11,
    safe_cast(ngay_12 as float64) as ngay_12,
    safe_cast(ngay_13 as float64) as ngay_13,
    safe_cast(ngay_14 as float64) as ngay_14,
    safe_cast(ngay_15 as float64) as ngay_15,
    safe_cast(ngay_16 as float64) as ngay_16,
    safe_cast(ngay_17 as float64) as ngay_17,
    safe_cast(ngay_18 as float64) as ngay_18,
    safe_cast(ngay_19 as float64) as ngay_19,
    safe_cast(ngay_20 as float64) as ngay_20,
    safe_cast(ngay_21 as float64) as ngay_21,
    safe_cast(ngay_22 as float64) as ngay_22,
    safe_cast(ngay_23 as float64) as ngay_23,
    safe_cast(ngay_24 as float64) as ngay_24,
    safe_cast(ngay_25 as float64) as ngay_25,
    safe_cast(ngay_26 as float64) as ngay_26,
    safe_cast(ngay_27 as float64) as ngay_27,
    safe_cast(ngay_28 as float64) as ngay_28,
    safe_cast(ngay_29 as float64) as ngay_29,
    safe_cast(ngay_30 as float64) as ngay_30,
    safe_cast(ngay_31 as float64) as ngay_31

  from src
  cross join params p

  where (
    (safe_cast(nam as int64) = p.curr_year and safe_cast(thang as int64) = p.curr_month)
    or (safe_cast(nam as int64) = p.prev_year and safe_cast(thang as int64) = p.prev_month)
  )
),

unpivoted as (
  select
    f.chi_tieu,
    f.danh_muc_san_pham,
    f.dong_san_pham,
    f.kenh,
    f.don_vi,
    f.nam,
    f.thang,
    x.ngay_int,
    x.gia_tri_raw
  from filtered f
  cross join unnest([
    struct(1 as ngay_int, f.ngay_1 as gia_tri_raw),
    struct(2 as ngay_int, f.ngay_2 as gia_tri_raw),
    struct(3 as ngay_int, f.ngay_3 as gia_tri_raw),
    struct(4 as ngay_int, f.ngay_4 as gia_tri_raw),
    struct(5 as ngay_int, f.ngay_5 as gia_tri_raw),
    struct(6 as ngay_int, f.ngay_6 as gia_tri_raw),
    struct(7 as ngay_int, f.ngay_7 as gia_tri_raw),
    struct(8 as ngay_int, f.ngay_8 as gia_tri_raw),
    struct(9 as ngay_int, f.ngay_9 as gia_tri_raw),
    struct(10 as ngay_int, f.ngay_10 as gia_tri_raw),
    struct(11 as ngay_int, f.ngay_11 as gia_tri_raw),
    struct(12 as ngay_int, f.ngay_12 as gia_tri_raw),
    struct(13 as ngay_int, f.ngay_13 as gia_tri_raw),
    struct(14 as ngay_int, f.ngay_14 as gia_tri_raw),
    struct(15 as ngay_int, f.ngay_15 as gia_tri_raw),
    struct(16 as ngay_int, f.ngay_16 as gia_tri_raw),
    struct(17 as ngay_int, f.ngay_17 as gia_tri_raw),
    struct(18 as ngay_int, f.ngay_18 as gia_tri_raw),
    struct(19 as ngay_int, f.ngay_19 as gia_tri_raw),
    struct(20 as ngay_int, f.ngay_20 as gia_tri_raw),
    struct(21 as ngay_int, f.ngay_21 as gia_tri_raw),
    struct(22 as ngay_int, f.ngay_22 as gia_tri_raw),
    struct(23 as ngay_int, f.ngay_23 as gia_tri_raw),
    struct(24 as ngay_int, f.ngay_24 as gia_tri_raw),
    struct(25 as ngay_int, f.ngay_25 as gia_tri_raw),
    struct(26 as ngay_int, f.ngay_26 as gia_tri_raw),
    struct(27 as ngay_int, f.ngay_27 as gia_tri_raw),
    struct(28 as ngay_int, f.ngay_28 as gia_tri_raw),
    struct(29 as ngay_int, f.ngay_29 as gia_tri_raw),
    struct(30 as ngay_int, f.ngay_30 as gia_tri_raw),
    struct(31 as ngay_int, f.ngay_31 as gia_tri_raw)
  ]) x
  where x.gia_tri_raw is not null
),

final as (
  select
    chi_tieu,
    danh_muc_san_pham,
    dong_san_pham,
    kenh,

    safe.parse_date(
      '%Y-%m-%d',
      format('%04d-%02d-%02d', nam, thang, ngay_int)
    ) as ngay_bao_cao,

    nam,
    thang,
    ngay_int as ngay,
    don_vi,

    case
      when regexp_contains(upper(chi_tieu), r'COST') then gia_tri_raw * 1.05
      else gia_tri_raw
    end as gia_tri,

    current_timestamp() as UpdateTime
  from unpivoted
)

select *
from final
where ngay_bao_cao is not null