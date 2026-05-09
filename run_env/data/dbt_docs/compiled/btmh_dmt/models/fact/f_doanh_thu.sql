





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
, month), interval 1 month) as end_excl,
    date '2025-08-31' as hd_legacy_end
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
),

nv_dim as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhan_vien`
),

the_dedup as (
  select * except(_rn)
  from (
    select
      the.*,
      row_number() over (
        partition by cast(the.Nguon as string), safe_cast(the.ID as int64)
        order by datetime(coalesce(the.LastEdit, the.InsertDate)) desc
      ) as _rn
    from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_the` the
  )
  where _rn = 1
),



src_ as (
  with
  vou as (
    select
      vou.*,
      row_number() over (
        partition by safe_cast(vou.ID as int64)
        order by safe_cast(vou.Stt as int64)
      ) as _rn
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`slbld` vou
    where cast(vou.Hs_Qd as string) = 'VOUCHER'
  )

  select
    concat(cast(m.ID as string), '-', cast(d.Stt as string), '') as ID,
    '' as Ma_Cong_Ty,
    safe_cast(m.ID_Dv as int64) as ID_Dv,
    safe_cast(m.ID as int64) as ID_Phieu_thu,
    date(m.Ngay) as Ngay,
    cast(m.Ngay as datetime) as Ngay_PhieuThu,
    safe_cast(d.Stt as int64) as STT_TrenPhieu,

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.Md as int64) as Ma_dong,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Thuc_Te,

    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Chi_TT,

    safe_cast(d.Don_Gia as bignumeric) as Don_gia_ban,
    safe_cast(d.T_Tien as bignumeric) as Thanh_tien_theo_DG_ban,
    safe_cast(d.T_Tien1 as bignumeric) as Gia_Von,

    safe_cast(d.TyLe_Giam as bignumeric) as TyLe_Giam_Gia_TP,
    safe_cast(d.Tien_Giam as bignumeric) as Tien_giam_gia_TP,
    safe_cast(d.Tien_CK as bignumeric) as CK_phan_bo,
    safe_cast(d.CK_The as bignumeric) as CK_the_phan_bo,
    safe_cast(d.CK_TheCn as bignumeric) as CK_TheCn_phan_bo,
    safe_cast(d.CK_TheMg as bignumeric) as CK_TheMg_phan_bo,

    (
      safe_cast(d.T_Tien as bignumeric)
      - (
        coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
        + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      )
    ) as Tien_PhieuThu1,

    (
      coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
      + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheCn as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheMg as bignumeric), 0)
    ) as Tien_Chiet_Khau,

    cast(m.Dien_Giai as string) as Dien_giai_Phieu_thu,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(m.Quay as string) as Quay,

    cast(nv.Ma_Nv as string) as Ma_Nv,
    cast(nvkt.Ma_Nv as string) as Ma_NvKT,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(csb_d.Ky_Hieu as string) as ctkm_kh,
    cast(csb_d.Noi_Dung as string) as ctkm_nd,
    cast(csb.Ky_Hieu as string) as ctkm_kh_pt,
    cast(csb.Noi_Dung as string) as ctkm_nd_pt,

    cast(the.So_The as string) as so_the_voucher,
    cast(the.Ten_The as string) as ten_the_voucher,

    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    cast(m.InsertDate as timestamp) as InsertDate,
    cast(m.Dc_Giao as string) as Dc_Giao,
    cast(m.Sub_ID as string) as Sub_ID,

    m.MarkRow,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`slbld` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`slblm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnv` nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnv` nvkt_raw
    on safe_cast(nvkt_raw.ID as int64) = safe_cast(m.UserIDXN as int64)

  left join nv_dim nvkt
    on cast(nvkt.Ma_Nv as string) = cast(nvkt_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`csb` csb
    on safe_cast(csb.ID as int64) = safe_cast(m.ID_CSB as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`csb` csb_d
    on safe_cast(csb_d.ID as int64) = safe_cast(d.ID_CSB as int64)

  left join vou
    on safe_cast(vou.ID as int64) = safe_cast(d.ID as int64)
    and vou._rn = 1
    and safe_cast(d.ID_CSB as int64) is not null
    and safe_cast(m.ID_CSB as int64) != safe_cast(d.ID_CSB as int64)

  left join the_dedup the
    on safe_cast(the.ID as int64) = safe_cast(vou.ID_The as int64)
    and cast(the.Nguon as string) = ''

  cross join params p
  where 1=1
    

    

    

    

    
)
,

src_ as (
  with
  vou as (
    select
      vou.*,
      row_number() over (
        partition by safe_cast(vou.ID as int64)
        order by safe_cast(vou.Stt as int64)
      ) as _rn
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`slbld` vou
    where cast(vou.Hs_Qd as string) = 'VOUCHER'
  )

  select
    concat(cast(m.ID as string), '-', cast(d.Stt as string), '') as ID,
    '' as Ma_Cong_Ty,
    safe_cast(m.ID_Dv as int64) as ID_Dv,
    safe_cast(m.ID as int64) as ID_Phieu_thu,
    date(m.Ngay) as Ngay,
    cast(m.Ngay as datetime) as Ngay_PhieuThu,
    safe_cast(d.Stt as int64) as STT_TrenPhieu,

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.Md as int64) as Ma_dong,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Thuc_Te,

    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Chi_TT,

    safe_cast(d.Don_Gia as bignumeric) as Don_gia_ban,
    safe_cast(d.T_Tien as bignumeric) as Thanh_tien_theo_DG_ban,
    safe_cast(d.T_Tien1 as bignumeric) as Gia_Von,

    safe_cast(d.TyLe_Giam as bignumeric) as TyLe_Giam_Gia_TP,
    safe_cast(d.Tien_Giam as bignumeric) as Tien_giam_gia_TP,
    safe_cast(d.Tien_CK as bignumeric) as CK_phan_bo,
    safe_cast(d.CK_The as bignumeric) as CK_the_phan_bo,
    safe_cast(d.CK_TheCn as bignumeric) as CK_TheCn_phan_bo,
    safe_cast(d.CK_TheMg as bignumeric) as CK_TheMg_phan_bo,

    (
      safe_cast(d.T_Tien as bignumeric)
      - (
        coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
        + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      )
    ) as Tien_PhieuThu1,

    (
      coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
      + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheCn as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheMg as bignumeric), 0)
    ) as Tien_Chiet_Khau,

    cast(m.Dien_Giai as string) as Dien_giai_Phieu_thu,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(m.Quay as string) as Quay,

    cast(nv.Ma_Nv as string) as Ma_Nv,
    cast(nvkt.Ma_Nv as string) as Ma_NvKT,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(csb_d.Ky_Hieu as string) as ctkm_kh,
    cast(csb_d.Noi_Dung as string) as ctkm_nd,
    cast(csb.Ky_Hieu as string) as ctkm_kh_pt,
    cast(csb.Noi_Dung as string) as ctkm_nd_pt,

    cast(the.So_The as string) as so_the_voucher,
    cast(the.Ten_The as string) as ten_the_voucher,

    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    cast(m.InsertDate as timestamp) as InsertDate,
    cast(m.Dc_Giao as string) as Dc_Giao,
    cast(m.Sub_ID as string) as Sub_ID,

    m.MarkRow,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`slbld` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`slblm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmnv` nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmnv` nvkt_raw
    on safe_cast(nvkt_raw.ID as int64) = safe_cast(m.UserIDXN as int64)

  left join nv_dim nvkt
    on cast(nvkt.Ma_Nv as string) = cast(nvkt_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`csb` csb
    on safe_cast(csb.ID as int64) = safe_cast(m.ID_CSB as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`csb` csb_d
    on safe_cast(csb_d.ID as int64) = safe_cast(d.ID_CSB as int64)

  left join vou
    on safe_cast(vou.ID as int64) = safe_cast(d.ID as int64)
    and vou._rn = 1
    and safe_cast(d.ID_CSB as int64) is not null
    and safe_cast(m.ID_CSB as int64) != safe_cast(d.ID_CSB as int64)

  left join the_dedup the
    on safe_cast(the.ID as int64) = safe_cast(vou.ID_The as int64)
    and cast(the.Nguon as string) = ''

  cross join params p
  where 1=1
    

    

    

    

    
)
,

src_ as (
  with
  vou as (
    select
      vou.*,
      row_number() over (
        partition by safe_cast(vou.ID as int64)
        order by safe_cast(vou.Stt as int64)
      ) as _rn
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`slbld` vou
    where cast(vou.Hs_Qd as string) = 'VOUCHER'
  )

  select
    concat(cast(m.ID as string), '-', cast(d.Stt as string), '') as ID,
    '' as Ma_Cong_Ty,
    safe_cast(m.ID_Dv as int64) as ID_Dv,
    safe_cast(m.ID as int64) as ID_Phieu_thu,
    date(m.Ngay) as Ngay,
    cast(m.Ngay as datetime) as Ngay_PhieuThu,
    safe_cast(d.Stt as int64) as STT_TrenPhieu,

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.Md as int64) as Ma_dong,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Thuc_Te,

    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Chi_TT,

    safe_cast(d.Don_Gia as bignumeric) as Don_gia_ban,
    safe_cast(d.T_Tien as bignumeric) as Thanh_tien_theo_DG_ban,
    safe_cast(d.T_Tien1 as bignumeric) as Gia_Von,

    safe_cast(d.TyLe_Giam as bignumeric) as TyLe_Giam_Gia_TP,
    safe_cast(d.Tien_Giam as bignumeric) as Tien_giam_gia_TP,
    safe_cast(d.Tien_CK as bignumeric) as CK_phan_bo,
    safe_cast(d.CK_The as bignumeric) as CK_the_phan_bo,
    safe_cast(d.CK_TheCn as bignumeric) as CK_TheCn_phan_bo,
    safe_cast(d.CK_TheMg as bignumeric) as CK_TheMg_phan_bo,

    (
      safe_cast(d.T_Tien as bignumeric)
      - (
        coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
        + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      )
    ) as Tien_PhieuThu1,

    (
      coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
      + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheCn as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheMg as bignumeric), 0)
    ) as Tien_Chiet_Khau,

    cast(m.Dien_Giai as string) as Dien_giai_Phieu_thu,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(m.Quay as string) as Quay,

    cast(nv.Ma_Nv as string) as Ma_Nv,
    cast(nvkt.Ma_Nv as string) as Ma_NvKT,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(csb_d.Ky_Hieu as string) as ctkm_kh,
    cast(csb_d.Noi_Dung as string) as ctkm_nd,
    cast(csb.Ky_Hieu as string) as ctkm_kh_pt,
    cast(csb.Noi_Dung as string) as ctkm_nd_pt,

    cast(the.So_The as string) as so_the_voucher,
    cast(the.Ten_The as string) as ten_the_voucher,

    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    cast(m.InsertDate as timestamp) as InsertDate,
    cast(m.Dc_Giao as string) as Dc_Giao,
    cast(m.Sub_ID as string) as Sub_ID,

    m.MarkRow,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`slbld` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`slblm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmnv` nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmnv` nvkt_raw
    on safe_cast(nvkt_raw.ID as int64) = safe_cast(m.UserIDXN as int64)

  left join nv_dim nvkt
    on cast(nvkt.Ma_Nv as string) = cast(nvkt_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`csb` csb
    on safe_cast(csb.ID as int64) = safe_cast(m.ID_CSB as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`csb` csb_d
    on safe_cast(csb_d.ID as int64) = safe_cast(d.ID_CSB as int64)

  left join vou
    on safe_cast(vou.ID as int64) = safe_cast(d.ID as int64)
    and vou._rn = 1
    and safe_cast(d.ID_CSB as int64) is not null
    and safe_cast(m.ID_CSB as int64) != safe_cast(d.ID_CSB as int64)

  left join the_dedup the
    on safe_cast(the.ID as int64) = safe_cast(vou.ID_The as int64)
    and cast(the.Nguon as string) = ''

  cross join params p
  where 1=1
    

    

    

    

    
)
,

src_ as (
  with
  vou as (
    select
      vou.*,
      row_number() over (
        partition by safe_cast(vou.ID as int64)
        order by safe_cast(vou.Stt as int64)
      ) as _rn
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`slbld` vou
    where cast(vou.Hs_Qd as string) = 'VOUCHER'
  )

  select
    concat(cast(m.ID as string), '-', cast(d.Stt as string), '') as ID,
    '' as Ma_Cong_Ty,
    safe_cast(m.ID_Dv as int64) as ID_Dv,
    safe_cast(m.ID as int64) as ID_Phieu_thu,
    date(m.Ngay) as Ngay,
    cast(m.Ngay as datetime) as Ngay_PhieuThu,
    safe_cast(d.Stt as int64) as STT_TrenPhieu,

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.Md as int64) as Ma_dong,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Thuc_Te,

    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Chi_TT,

    safe_cast(d.Don_Gia as bignumeric) as Don_gia_ban,
    safe_cast(d.T_Tien as bignumeric) as Thanh_tien_theo_DG_ban,
    safe_cast(d.T_Tien1 as bignumeric) as Gia_Von,

    safe_cast(d.TyLe_Giam as bignumeric) as TyLe_Giam_Gia_TP,
    safe_cast(d.Tien_Giam as bignumeric) as Tien_giam_gia_TP,
    safe_cast(d.Tien_CK as bignumeric) as CK_phan_bo,
    safe_cast(d.CK_The as bignumeric) as CK_the_phan_bo,
    safe_cast(d.CK_TheCn as bignumeric) as CK_TheCn_phan_bo,
    safe_cast(d.CK_TheMg as bignumeric) as CK_TheMg_phan_bo,

    (
      safe_cast(d.T_Tien as bignumeric)
      - (
        coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
        + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      )
    ) as Tien_PhieuThu1,

    (
      coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
      + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheCn as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheMg as bignumeric), 0)
    ) as Tien_Chiet_Khau,

    cast(m.Dien_Giai as string) as Dien_giai_Phieu_thu,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(m.Quay as string) as Quay,

    cast(nv.Ma_Nv as string) as Ma_Nv,
    cast(nvkt.Ma_Nv as string) as Ma_NvKT,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(csb_d.Ky_Hieu as string) as ctkm_kh,
    cast(csb_d.Noi_Dung as string) as ctkm_nd,
    cast(csb.Ky_Hieu as string) as ctkm_kh_pt,
    cast(csb.Noi_Dung as string) as ctkm_nd_pt,

    cast(the.So_The as string) as so_the_voucher,
    cast(the.Ten_The as string) as ten_the_voucher,

    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    cast(m.InsertDate as timestamp) as InsertDate,
    cast(m.Dc_Giao as string) as Dc_Giao,
    cast(m.Sub_ID as string) as Sub_ID,

    m.MarkRow,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`slbld` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`slblm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmnv` nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmnv` nvkt_raw
    on safe_cast(nvkt_raw.ID as int64) = safe_cast(m.UserIDXN as int64)

  left join nv_dim nvkt
    on cast(nvkt.Ma_Nv as string) = cast(nvkt_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`csb` csb
    on safe_cast(csb.ID as int64) = safe_cast(m.ID_CSB as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`csb` csb_d
    on safe_cast(csb_d.ID as int64) = safe_cast(d.ID_CSB as int64)

  left join vou
    on safe_cast(vou.ID as int64) = safe_cast(d.ID as int64)
    and vou._rn = 1
    and safe_cast(d.ID_CSB as int64) is not null
    and safe_cast(m.ID_CSB as int64) != safe_cast(d.ID_CSB as int64)

  left join the_dedup the
    on safe_cast(the.ID as int64) = safe_cast(vou.ID_The as int64)
    and cast(the.Nguon as string) = ''

  cross join params p
  where 1=1
    

    

    

    

    
)
,

src_ as (
  with
  vou as (
    select
      vou.*,
      row_number() over (
        partition by safe_cast(vou.ID as int64)
        order by safe_cast(vou.Stt as int64)
      ) as _rn
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`slbld` vou
    where cast(vou.Hs_Qd as string) = 'VOUCHER'
  )

  select
    concat(cast(m.ID as string), '-', cast(d.Stt as string), '') as ID,
    '' as Ma_Cong_Ty,
    safe_cast(m.ID_Dv as int64) as ID_Dv,
    safe_cast(m.ID as int64) as ID_Phieu_thu,
    date(m.Ngay) as Ngay,
    cast(m.Ngay as datetime) as Ngay_PhieuThu,
    safe_cast(d.Stt as int64) as STT_TrenPhieu,

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.Md as int64) as Ma_dong,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Thuc_Te,

    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    (
      
(
  case
    when (
      cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(nh.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 4) in ('TTSJ', 'TTVR') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD', '24', 'VT') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        else (
          safe_cast(d.So_Luong as bignumeric) * (
            coalesce(safe_cast(h.T_Luong as bignumeric), 0)
            + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
            + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
            + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
          )
        )
      end
    )
    else safe_cast(d.So_Luong as bignumeric)
  end
)

    ) as SL_Chi_TT,

    safe_cast(d.Don_Gia as bignumeric) as Don_gia_ban,
    safe_cast(d.T_Tien as bignumeric) as Thanh_tien_theo_DG_ban,
    safe_cast(d.T_Tien1 as bignumeric) as Gia_Von,

    safe_cast(d.TyLe_Giam as bignumeric) as TyLe_Giam_Gia_TP,
    safe_cast(d.Tien_Giam as bignumeric) as Tien_giam_gia_TP,
    safe_cast(d.Tien_CK as bignumeric) as CK_phan_bo,
    safe_cast(d.CK_The as bignumeric) as CK_the_phan_bo,
    safe_cast(d.CK_TheCn as bignumeric) as CK_TheCn_phan_bo,
    safe_cast(d.CK_TheMg as bignumeric) as CK_TheMg_phan_bo,

    (
      safe_cast(d.T_Tien as bignumeric)
      - (
        coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
        + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      )
    ) as Tien_PhieuThu1,

    (
      coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
      + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheCn as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheMg as bignumeric), 0)
    ) as Tien_Chiet_Khau,

    cast(m.Dien_Giai as string) as Dien_giai_Phieu_thu,

    
    cast(kho.Ma_Kho as string) as Ma_Kho,
    

    cast(m.Quay as string) as Quay,

    cast(nv.Ma_Nv as string) as Ma_Nv,
    cast(nvkt.Ma_Nv as string) as Ma_NvKT,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(csb_d.Ky_Hieu as string) as ctkm_kh,
    cast(csb_d.Noi_Dung as string) as ctkm_nd,
    cast(csb.Ky_Hieu as string) as ctkm_kh_pt,
    cast(csb.Noi_Dung as string) as ctkm_nd_pt,

    cast(the.So_The as string) as so_the_voucher,
    cast(the.Ten_The as string) as ten_the_voucher,

    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    cast(m.InsertDate as timestamp) as InsertDate,
    cast(m.Dc_Giao as string) as Dc_Giao,
    cast(m.Sub_ID as string) as Sub_ID,

    m.MarkRow,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`slbld` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`slblm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmnv` nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmnv` nvkt_raw
    on safe_cast(nvkt_raw.ID as int64) = safe_cast(m.UserIDXN as int64)

  left join nv_dim nvkt
    on cast(nvkt.Ma_Nv as string) = cast(nvkt_raw.Ma_Nv as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`csb` csb
    on safe_cast(csb.ID as int64) = safe_cast(m.ID_CSB as int64)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`csb` csb_d
    on safe_cast(csb_d.ID as int64) = safe_cast(d.ID_CSB as int64)

  left join vou
    on safe_cast(vou.ID as int64) = safe_cast(d.ID as int64)
    and vou._rn = 1
    and safe_cast(d.ID_CSB as int64) is not null
    and safe_cast(m.ID_CSB as int64) != safe_cast(d.ID_CSB as int64)

  left join the_dedup the
    on safe_cast(the.ID as int64) = safe_cast(vou.ID_The as int64)
    and cast(the.Nguon as string) = ''

  cross join params p
  where 1=1
    

    

    

    

    
)



select * from (

  select * from src_
  union all

  select * from src_
  union all

  select * from src_
  union all

  select * from src_
  union all

  select * from src_
  

)