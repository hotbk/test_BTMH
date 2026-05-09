




with params as (
  select
    
(
  
    
(
  
    current_date()
  
)

  
)
 as run_date,
    
(
  
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 6 month)
  
)

  
)
 as start_date,
    date_trunc(
(
  
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 6 month)
  
)

  
)
, month) as start_month,
    date_add(date_trunc(
(
  
    
(
  
    current_date()
  
)

  
)
, month), interval 1 month) as end_excl
),

dmh as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang`
),

kho_dim as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_kho`
),

dt_dim as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_doi_tac`
)



, src_ny as (
  select
    cast(d.ID as string) as ID,
    safe_cast(d.Stt as int64) as Stt,
    'NY' as Nguon,

    safe_cast(date(m.Ngay) as date) as Ngay,

    
      cast(khox.Ma_Kho as string) as Ma_KhoX,
      cast(khon.Ma_Kho as string) as Ma_KhoN,
      cast(khond.Ma_Kho as string) as Ma_KhoND,
    

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    cast(
      (
        coalesce(safe_cast(h.T_Luong as bignumeric), 0)
        + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
        + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
        + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
      ) as bignumeric
    ) as Tong_tlg,

    safe_cast(d.So_Luong as bignumeric) as So_luong,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,
    safe_cast(m.Ty_Gia as bignumeric) as Ty_gia,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    cast(d.Ghi_Chu as string) as Ghi_Chu,
    cast(m.Ky_Hieu as string) as Ky_Hieu,
    cast(m.So_Ct as string) as So_Ct,
    safe_cast(date(m.Ngay_Ct) as date) as Ngay_Ct,
    cast(m.So_Bk as string) as So_Bk,
    cast(m.Sp as string) as Sp,
    safe_cast(m.ID_Tt as int64) as ID_Tt,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(m.Dien_Giai as string) as Dien_Giai,

    safe_cast(date(m.NgayXN) as date) as NgayXN,
    cast(m.UserIDXN as string) as UserIDXN,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`sldcd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`sldcm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho` khox_raw
    on safe_cast(khox_raw.ID as int64) = safe_cast(d.ID_KhoX as int64)

  left join kho_dim khox
    on cast(khox.Ma_Kho as string) = cast(khox_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho` khon_raw
    on safe_cast(khon_raw.ID as int64) = safe_cast(d.ID_KhoN as int64)

  left join kho_dim khon
    on cast(khon.Ma_Kho as string) = cast(khon_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho` khond_raw
    on safe_cast(khond_raw.ID as int64) = safe_cast(m.ID_KhoN as int64)

  left join kho_dim khond
    on cast(khond.Ma_Kho as string) = cast(khond_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where safe_cast(m.ID_Dv as int64) >= 0
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_sx as (
  select
    cast(d.ID as string) as ID,
    safe_cast(d.Stt as int64) as Stt,
    'SX' as Nguon,

    safe_cast(date(m.Ngay) as date) as Ngay,

    
      cast(khox.Ma_Kho as string) as Ma_KhoX,
      cast(khon.Ma_Kho as string) as Ma_KhoN,
      cast(khond.Ma_Kho as string) as Ma_KhoND,
    

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    cast(
      (
        coalesce(safe_cast(h.T_Luong as bignumeric), 0)
        + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
        + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
        + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
      ) as bignumeric
    ) as Tong_tlg,

    safe_cast(d.So_Luong as bignumeric) as So_luong,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,
    safe_cast(m.Ty_Gia as bignumeric) as Ty_gia,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    cast(d.Ghi_Chu as string) as Ghi_Chu,
    cast(m.Ky_Hieu as string) as Ky_Hieu,
    cast(m.So_Ct as string) as So_Ct,
    safe_cast(date(m.Ngay_Ct) as date) as Ngay_Ct,
    cast(m.So_Bk as string) as So_Bk,
    cast(m.Sp as string) as Sp,
    safe_cast(m.ID_Tt as int64) as ID_Tt,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(m.Dien_Giai as string) as Dien_Giai,

    safe_cast(date(m.NgayXN) as date) as NgayXN,
    cast(m.UserIDXN as string) as UserIDXN,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`sldcd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`sldcm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho` khox_raw
    on safe_cast(khox_raw.ID as int64) = safe_cast(d.ID_KhoX as int64)

  left join kho_dim khox
    on cast(khox.Ma_Kho as string) = cast(khox_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho` khon_raw
    on safe_cast(khon_raw.ID as int64) = safe_cast(d.ID_KhoN as int64)

  left join kho_dim khon
    on cast(khon.Ma_Kho as string) = cast(khon_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho` khond_raw
    on safe_cast(khond_raw.ID as int64) = safe_cast(m.ID_KhoN as int64)

  left join kho_dim khond
    on cast(khond.Ma_Kho as string) = cast(khond_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where safe_cast(m.ID_Dv as int64) >= 0
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_hd as (
  select
    cast(d.ID as string) as ID,
    safe_cast(d.Stt as int64) as Stt,
    'HD' as Nguon,

    safe_cast(date(m.Ngay) as date) as Ngay,

    
      cast(khox.Ma_Kho as string) as Ma_KhoX,
      cast(khon.Ma_Kho as string) as Ma_KhoN,
      cast(khond.Ma_Kho as string) as Ma_KhoND,
    

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    cast(
      (
        coalesce(safe_cast(h.T_Luong as bignumeric), 0)
        + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
        + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
        + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
      ) as bignumeric
    ) as Tong_tlg,

    safe_cast(d.So_Luong as bignumeric) as So_luong,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,
    safe_cast(m.Ty_Gia as bignumeric) as Ty_gia,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    cast(d.Ghi_Chu as string) as Ghi_Chu,
    cast(m.Ky_Hieu as string) as Ky_Hieu,
    cast(m.So_Ct as string) as So_Ct,
    safe_cast(date(m.Ngay_Ct) as date) as Ngay_Ct,
    cast(m.So_Bk as string) as So_Bk,
    cast(m.Sp as string) as Sp,
    safe_cast(m.ID_Tt as int64) as ID_Tt,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(m.Dien_Giai as string) as Dien_Giai,

    safe_cast(date(m.NgayXN) as date) as NgayXN,
    cast(m.UserIDXN as string) as UserIDXN,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`sldcd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`sldcm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho` khox_raw
    on safe_cast(khox_raw.ID as int64) = safe_cast(d.ID_KhoX as int64)

  left join kho_dim khox
    on cast(khox.Ma_Kho as string) = cast(khox_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho` khon_raw
    on safe_cast(khon_raw.ID as int64) = safe_cast(d.ID_KhoN as int64)

  left join kho_dim khon
    on cast(khon.Ma_Kho as string) = cast(khon_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho` khond_raw
    on safe_cast(khond_raw.ID as int64) = safe_cast(m.ID_KhoN as int64)

  left join kho_dim khond
    on cast(khond.Ma_Kho as string) = cast(khond_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where safe_cast(m.ID_Dv as int64) >= 0
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_bn as (
  select
    cast(d.ID as string) as ID,
    safe_cast(d.Stt as int64) as Stt,
    'BN' as Nguon,

    safe_cast(date(m.Ngay) as date) as Ngay,

    
      cast(khox.Ma_Kho as string) as Ma_KhoX,
      cast(khon.Ma_Kho as string) as Ma_KhoN,
      cast(khond.Ma_Kho as string) as Ma_KhoND,
    

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    cast(
      (
        coalesce(safe_cast(h.T_Luong as bignumeric), 0)
        + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
        + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
        + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
      ) as bignumeric
    ) as Tong_tlg,

    safe_cast(d.So_Luong as bignumeric) as So_luong,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,
    safe_cast(m.Ty_Gia as bignumeric) as Ty_gia,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    cast(d.Ghi_Chu as string) as Ghi_Chu,
    cast(m.Ky_Hieu as string) as Ky_Hieu,
    cast(m.So_Ct as string) as So_Ct,
    safe_cast(date(m.Ngay_Ct) as date) as Ngay_Ct,
    cast(m.So_Bk as string) as So_Bk,
    cast(m.Sp as string) as Sp,
    safe_cast(m.ID_Tt as int64) as ID_Tt,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(m.Dien_Giai as string) as Dien_Giai,

    safe_cast(date(m.NgayXN) as date) as NgayXN,
    cast(m.UserIDXN as string) as UserIDXN,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`sldcd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`sldcm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho` khox_raw
    on safe_cast(khox_raw.ID as int64) = safe_cast(d.ID_KhoX as int64)

  left join kho_dim khox
    on cast(khox.Ma_Kho as string) = cast(khox_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho` khon_raw
    on safe_cast(khon_raw.ID as int64) = safe_cast(d.ID_KhoN as int64)

  left join kho_dim khon
    on cast(khon.Ma_Kho as string) = cast(khon_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho` khond_raw
    on safe_cast(khond_raw.ID as int64) = safe_cast(m.ID_KhoN as int64)

  left join kho_dim khond
    on cast(khond.Ma_Kho as string) = cast(khond_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where safe_cast(m.ID_Dv as int64) >= 0
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_sg as (
  select
    cast(d.ID as string) as ID,
    safe_cast(d.Stt as int64) as Stt,
    'SG' as Nguon,

    safe_cast(date(m.Ngay) as date) as Ngay,

    
      cast(khox.Ma_Kho as string) as Ma_KhoX,
      cast(khon.Ma_Kho as string) as Ma_KhoN,
      cast(khond.Ma_Kho as string) as Ma_KhoND,
    

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    cast(
      (
        coalesce(safe_cast(h.T_Luong as bignumeric), 0)
        + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
        + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
        + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
      ) as bignumeric
    ) as Tong_tlg,

    safe_cast(d.So_Luong as bignumeric) as So_luong,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,
    safe_cast(m.Ty_Gia as bignumeric) as Ty_gia,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    cast(d.Ghi_Chu as string) as Ghi_Chu,
    cast(m.Ky_Hieu as string) as Ky_Hieu,
    cast(m.So_Ct as string) as So_Ct,
    safe_cast(date(m.Ngay_Ct) as date) as Ngay_Ct,
    cast(m.So_Bk as string) as So_Bk,
    cast(m.Sp as string) as Sp,
    safe_cast(m.ID_Tt as int64) as ID_Tt,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(m.Dien_Giai as string) as Dien_Giai,

    safe_cast(date(m.NgayXN) as date) as NgayXN,
    cast(m.UserIDXN as string) as UserIDXN,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`sldcd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`sldcm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho` khox_raw
    on safe_cast(khox_raw.ID as int64) = safe_cast(d.ID_KhoX as int64)

  left join kho_dim khox
    on cast(khox.Ma_Kho as string) = cast(khox_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho` khon_raw
    on safe_cast(khon_raw.ID as int64) = safe_cast(d.ID_KhoN as int64)

  left join kho_dim khon
    on cast(khon.Ma_Kho as string) = cast(khon_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho` khond_raw
    on safe_cast(khond_raw.ID as int64) = safe_cast(m.ID_KhoN as int64)

  left join kho_dim khond
    on cast(khond.Ma_Kho as string) = cast(khond_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where safe_cast(m.ID_Dv as int64) >= 0
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)


select *
from (

  select * from src_ny
  union all

  select * from src_sx
  union all

  select * from src_hd
  union all

  select * from src_bn
  union all

  select * from src_sg
  

)

