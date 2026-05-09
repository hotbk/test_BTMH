



with params as (
  select
    extract(year from 
(
  
    current_date()
  
)
) as run_year,
    extract(month from 
(
  
    current_date()
  
)
) as run_month
),

dmh as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang`
),

kho_dim as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_kho`
),



src_ny as (
  select
    'NY' as Nguon,
    cast(htk.Nam as int64) as Nam,
    cast(htk.Mm as int64) as Mm,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(htk.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as float64), 0)
      + coalesce(safe_cast(h.The_Tich as float64), 0)
      + coalesce(safe_cast(h.Tien_Lai as float64), 0)
      + coalesce(safe_cast(h.Tyle_Lai as float64), 0)
    ) as Tong_tlg,

    safe_cast(htk.Don_Gia1 as float64) as Don_Gia1,
    safe_cast(htk.T_Tien1 as float64) as T_Tien1,
    safe_cast(htk.So_Luong as float64) as So_Luong,
    safe_cast(htk.Sl_Qd as float64) as Sl_Qd,
    safe_cast(htk.Gia_NM as float64) as Gia_NM,

    safe.parse_date('%Y%m%d', concat('20', cast(htk.Sngay as string))) as Ngay_Nhap,

    current_timestamp() as UpdateTime
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`htk` htk
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho` kho_raw
    on safe_cast(htk.ID_Kho as int64) = safe_cast(kho_raw.ID as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)
  left join dmh h
    on safe_cast(htk.ID_Hang as int64) = safe_cast(h.ID as int64)
  cross join params p
  where safe_cast(htk.Nam as int64) = p.run_year
    and safe_cast(htk.Mm as int64) = p.run_month
)
,

src_sx as (
  select
    'SX' as Nguon,
    cast(htk.Nam as int64) as Nam,
    cast(htk.Mm as int64) as Mm,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(htk.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as float64), 0)
      + coalesce(safe_cast(h.The_Tich as float64), 0)
      + coalesce(safe_cast(h.Tien_Lai as float64), 0)
      + coalesce(safe_cast(h.Tyle_Lai as float64), 0)
    ) as Tong_tlg,

    safe_cast(htk.Don_Gia1 as float64) as Don_Gia1,
    safe_cast(htk.T_Tien1 as float64) as T_Tien1,
    safe_cast(htk.So_Luong as float64) as So_Luong,
    safe_cast(htk.Sl_Qd as float64) as Sl_Qd,
    safe_cast(htk.Gia_NM as float64) as Gia_NM,

    safe.parse_date('%Y%m%d', concat('20', cast(htk.Sngay as string))) as Ngay_Nhap,

    current_timestamp() as UpdateTime
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`htk` htk
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho` kho_raw
    on safe_cast(htk.ID_Kho as int64) = safe_cast(kho_raw.ID as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)
  left join dmh h
    on safe_cast(htk.ID_Hang as int64) = safe_cast(h.ID as int64)
  cross join params p
  where safe_cast(htk.Nam as int64) = p.run_year
    and safe_cast(htk.Mm as int64) = p.run_month
)
,

src_hd as (
  select
    'HD' as Nguon,
    cast(htk.Nam as int64) as Nam,
    cast(htk.Mm as int64) as Mm,

    
    case
      
      when cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
      when cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
      
      else cast(kho.Ma_Kho as string)
    end as Ma_Kho,
    

    cast(htk.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as float64), 0)
      + coalesce(safe_cast(h.The_Tich as float64), 0)
      + coalesce(safe_cast(h.Tien_Lai as float64), 0)
      + coalesce(safe_cast(h.Tyle_Lai as float64), 0)
    ) as Tong_tlg,

    safe_cast(htk.Don_Gia1 as float64) as Don_Gia1,
    safe_cast(htk.T_Tien1 as float64) as T_Tien1,
    safe_cast(htk.So_Luong as float64) as So_Luong,
    safe_cast(htk.Sl_Qd as float64) as Sl_Qd,
    safe_cast(htk.Gia_NM as float64) as Gia_NM,

    safe.parse_date('%Y%m%d', concat('20', cast(htk.Sngay as string))) as Ngay_Nhap,

    current_timestamp() as UpdateTime
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`htk` htk
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho` kho_raw
    on safe_cast(htk.ID_Kho as int64) = safe_cast(kho_raw.ID as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)
  left join dmh h
    on safe_cast(htk.ID_Hang as int64) = safe_cast(h.ID as int64)
  cross join params p
  where safe_cast(htk.Nam as int64) = p.run_year
    and safe_cast(htk.Mm as int64) = p.run_month
)
,

src_bn as (
  select
    'BN' as Nguon,
    cast(htk.Nam as int64) as Nam,
    cast(htk.Mm as int64) as Mm,

    
    case
      
      when cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
      when cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
      
      else cast(kho.Ma_Kho as string)
    end as Ma_Kho,
    

    cast(htk.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as float64), 0)
      + coalesce(safe_cast(h.The_Tich as float64), 0)
      + coalesce(safe_cast(h.Tien_Lai as float64), 0)
      + coalesce(safe_cast(h.Tyle_Lai as float64), 0)
    ) as Tong_tlg,

    safe_cast(htk.Don_Gia1 as float64) as Don_Gia1,
    safe_cast(htk.T_Tien1 as float64) as T_Tien1,
    safe_cast(htk.So_Luong as float64) as So_Luong,
    safe_cast(htk.Sl_Qd as float64) as Sl_Qd,
    safe_cast(htk.Gia_NM as float64) as Gia_NM,

    safe.parse_date('%Y%m%d', concat('20', cast(htk.Sngay as string))) as Ngay_Nhap,

    current_timestamp() as UpdateTime
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`htk` htk
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho` kho_raw
    on safe_cast(htk.ID_Kho as int64) = safe_cast(kho_raw.ID as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)
  left join dmh h
    on safe_cast(htk.ID_Hang as int64) = safe_cast(h.ID as int64)
  cross join params p
  where safe_cast(htk.Nam as int64) = p.run_year
    and safe_cast(htk.Mm as int64) = p.run_month
)
,

src_sg as (
  select
    'SG' as Nguon,
    cast(htk.Nam as int64) as Nam,
    cast(htk.Mm as int64) as Mm,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(htk.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as float64), 0)
      + coalesce(safe_cast(h.The_Tich as float64), 0)
      + coalesce(safe_cast(h.Tien_Lai as float64), 0)
      + coalesce(safe_cast(h.Tyle_Lai as float64), 0)
    ) as Tong_tlg,

    safe_cast(htk.Don_Gia1 as float64) as Don_Gia1,
    safe_cast(htk.T_Tien1 as float64) as T_Tien1,
    safe_cast(htk.So_Luong as float64) as So_Luong,
    safe_cast(htk.Sl_Qd as float64) as Sl_Qd,
    safe_cast(htk.Gia_NM as float64) as Gia_NM,

    safe.parse_date('%Y%m%d', concat('20', cast(htk.Sngay as string))) as Ngay_Nhap,

    current_timestamp() as UpdateTime
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`htk` htk
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho` kho_raw
    on safe_cast(htk.ID_Kho as int64) = safe_cast(kho_raw.ID as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)
  left join dmh h
    on safe_cast(htk.ID_Hang as int64) = safe_cast(h.ID as int64)
  cross join params p
  where safe_cast(htk.Nam as int64) = p.run_year
    and safe_cast(htk.Mm as int64) = p.run_month
)



select * from (

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