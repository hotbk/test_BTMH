




with params as (
  select
    
    (
      
        
(
  
    
(
  
    current_date()
  
)

  
)

      
    ) as end_date,
    (
      
        
(
  
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 6 month)
  
)

  
)

      
    ) as start_date,
    date_trunc(
      (
        
          
(
  
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 6 month)
  
)

  
)

        
      ),
      month
    ) as start_month,
    date_add(date_trunc(
      (
        
          
(
  
    
(
  
    current_date()
  
)

  
)

        
      ),
      month
    ), interval 1 month) as end_excl
),

dmh as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang`
),

ht as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan`
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
    'NY' as Nguon,

    safe_cast(m.ID_Dv as int64) as ID_Dv,

    cast(d.ID as string) as ID,
    safe_cast(m.ID_Nx as int64) as ID_Nx,
    cast(ht.Ma_HT as string) as Ma_HT,
    safe_cast(d.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
    ) as Tong_tlg,

    (
      
(
  case
    when (
      cast(h.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
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

    safe_cast(d.So_Luong as bignumeric) as So_Luong_Theo_Dvt,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    safe_cast(d.Gia_Ban as bignumeric) as Gia_Ban,
    safe_cast(d.Gia_VAT as bignumeric) as Gia_VAT,
    safe_cast(d.Gia_KVat as bignumeric) as Gia_KVat,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,

    safe_cast(d.Don_Gia1 as bignumeric) as Don_Gia1,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Ct) as Ngay_Ct,
    cast(m.Sp as string) as Sp,

    (
      case
        when 'NY' = 'HD' and cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
        when 'NY' = 'HD' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
        when 'NY' = 'BN' and cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
        when 'NY' = 'BN' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(kho.Ma_Kho as string)
      end
    ) as Ma_Kho,

    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,
    cast(m.Sub_ID as string) as Sub_ID,

    cast(m.So_Ct as string) as So_Ct,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`slnxd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`slnxm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join ht
    on safe_cast(ht.ID as int64) = safe_cast(m.ID_Nx as int64)
    and cast(ht.Nguon as string) = 'NY'

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(d.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where 1=1
    and d.ID_Hang is not null
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_sx as (
  select
    'SX' as Nguon,

    safe_cast(m.ID_Dv as int64) as ID_Dv,

    cast(d.ID as string) as ID,
    safe_cast(m.ID_Nx as int64) as ID_Nx,
    cast(ht.Ma_HT as string) as Ma_HT,
    safe_cast(d.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
    ) as Tong_tlg,

    (
      
(
  case
    when (
      cast(h.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
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

    safe_cast(d.So_Luong as bignumeric) as So_Luong_Theo_Dvt,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    safe_cast(d.Gia_Ban as bignumeric) as Gia_Ban,
    safe_cast(d.Gia_VAT as bignumeric) as Gia_VAT,
    safe_cast(d.Gia_KVat as bignumeric) as Gia_KVat,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,

    safe_cast(d.Don_Gia1 as bignumeric) as Don_Gia1,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Ct) as Ngay_Ct,
    cast(m.Sp as string) as Sp,

    (
      case
        when 'SX' = 'HD' and cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
        when 'SX' = 'HD' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
        when 'SX' = 'BN' and cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
        when 'SX' = 'BN' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(kho.Ma_Kho as string)
      end
    ) as Ma_Kho,

    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,
    cast(m.Sub_ID as string) as Sub_ID,

    cast(m.So_Ct as string) as So_Ct,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`slnxd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`slnxm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join ht
    on safe_cast(ht.ID as int64) = safe_cast(m.ID_Nx as int64)
    and cast(ht.Nguon as string) = 'SX'

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(d.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where 1=1
    and d.ID_Hang is not null
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_hd as (
  select
    'HD' as Nguon,

    safe_cast(m.ID_Dv as int64) as ID_Dv,

    cast(d.ID as string) as ID,
    safe_cast(m.ID_Nx as int64) as ID_Nx,
    cast(ht.Ma_HT as string) as Ma_HT,
    safe_cast(d.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
    ) as Tong_tlg,

    (
      
(
  case
    when (
      cast(h.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
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

    safe_cast(d.So_Luong as bignumeric) as So_Luong_Theo_Dvt,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    safe_cast(d.Gia_Ban as bignumeric) as Gia_Ban,
    safe_cast(d.Gia_VAT as bignumeric) as Gia_VAT,
    safe_cast(d.Gia_KVat as bignumeric) as Gia_KVat,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,

    safe_cast(d.Don_Gia1 as bignumeric) as Don_Gia1,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Ct) as Ngay_Ct,
    cast(m.Sp as string) as Sp,

    (
      case
        when 'HD' = 'HD' and cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
        when 'HD' = 'HD' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
        when 'HD' = 'BN' and cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
        when 'HD' = 'BN' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(kho.Ma_Kho as string)
      end
    ) as Ma_Kho,

    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,
    cast(m.Sub_ID as string) as Sub_ID,

    cast(m.So_Ct as string) as So_Ct,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`slnxd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`slnxm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join ht
    on safe_cast(ht.ID as int64) = safe_cast(m.ID_Nx as int64)
    and cast(ht.Nguon as string) = 'HD'

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(d.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where 1=1
    and d.ID_Hang is not null
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_bn as (
  select
    'BN' as Nguon,

    safe_cast(m.ID_Dv as int64) as ID_Dv,

    cast(d.ID as string) as ID,
    safe_cast(m.ID_Nx as int64) as ID_Nx,
    cast(ht.Ma_HT as string) as Ma_HT,
    safe_cast(d.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
    ) as Tong_tlg,

    (
      
(
  case
    when (
      cast(h.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
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

    safe_cast(d.So_Luong as bignumeric) as So_Luong_Theo_Dvt,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    safe_cast(d.Gia_Ban as bignumeric) as Gia_Ban,
    safe_cast(d.Gia_VAT as bignumeric) as Gia_VAT,
    safe_cast(d.Gia_KVat as bignumeric) as Gia_KVat,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,

    safe_cast(d.Don_Gia1 as bignumeric) as Don_Gia1,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Ct) as Ngay_Ct,
    cast(m.Sp as string) as Sp,

    (
      case
        when 'BN' = 'HD' and cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
        when 'BN' = 'HD' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
        when 'BN' = 'BN' and cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
        when 'BN' = 'BN' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(kho.Ma_Kho as string)
      end
    ) as Ma_Kho,

    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,
    cast(m.Sub_ID as string) as Sub_ID,

    cast(m.So_Ct as string) as So_Ct,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`slnxd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`slnxm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join ht
    on safe_cast(ht.ID as int64) = safe_cast(m.ID_Nx as int64)
    and cast(ht.Nguon as string) = 'BN'

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(d.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where 1=1
    and d.ID_Hang is not null
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)

, src_sg as (
  select
    'SG' as Nguon,

    safe_cast(m.ID_Dv as int64) as ID_Dv,

    cast(d.ID as string) as ID,
    safe_cast(m.ID_Nx as int64) as ID_Nx,
    cast(ht.Ma_HT as string) as Ma_HT,
    safe_cast(d.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
    ) as Tong_tlg,

    (
      
(
  case
    when (
      cast(h.Ma_Nhom as string) in ('NL24', 'NLTT')
      or substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV','NTQ', 'NGB')
      or cast(h.Ma_Hang as string) in ('BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001')
    ) then (
      case
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
        when substr(cast(h.Ma_Nhom as string), 1, 3) in ('NTQ', 'NGB') then (safe_cast(d.So_Luong as bignumeric) * coalesce(safe_cast(h.T_Luong as bignumeric), 0))
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

    safe_cast(d.So_Luong as bignumeric) as So_Luong_Theo_Dvt,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    safe_cast(d.Gia_Ban as bignumeric) as Gia_Ban,
    safe_cast(d.Gia_VAT as bignumeric) as Gia_VAT,
    safe_cast(d.Gia_KVat as bignumeric) as Gia_KVat,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,

    safe_cast(d.Don_Gia1 as bignumeric) as Don_Gia1,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Ct) as Ngay_Ct,
    cast(m.Sp as string) as Sp,

    (
      case
        when 'SG' = 'HD' and cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
        when 'SG' = 'HD' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
        when 'SG' = 'BN' and cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
        when 'SG' = 'BN' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(kho.Ma_Kho as string)
      end
    ) as Ma_Kho,

    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,
    cast(m.Sub_ID as string) as Sub_ID,

    cast(m.So_Ct as string) as So_Ct,

    current_timestamp() as UpdateTime

  from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`slnxd` d
  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`slnxm` m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join ht
    on safe_cast(ht.ID as int64) = safe_cast(m.ID_Nx as int64)
    and cast(ht.Nguon as string) = 'SG'

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho` kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(d.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmdt` dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where 1=1
    and d.ID_Hang is not null
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)


select t.* from (

  select * from src_ny
  union all

  select * from src_sx
  union all

  select * from src_hd
  union all

  select * from src_bn
  union all

  select * from src_sg
  

) t
cross join params p

