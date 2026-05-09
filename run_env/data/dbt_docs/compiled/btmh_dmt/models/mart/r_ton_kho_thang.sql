



with params as (
  select
    
(
  
    current_date()
  
)
 as run_date,
    date_trunc(
(
  
    current_date()
  
)
, month) as start_of_month,
    date_diff(
(
  
    current_date()
  
)
, date_trunc(
(
  
    current_date()
  
)
, month), day) as days_diff,
    safe_divide(
      cast(date_diff(
(
  
    current_date()
  
)
, date_trunc(
(
  
    current_date()
  
)
, month), day) as bignumeric),
      cast((date_diff(
(
  
    current_date()
  
)
, date_trunc(
(
  
    current_date()
  
)
, month), day) + 1) as bignumeric)
    ) as days_factor
),

-- 2.1 HTK: tồn kho đầu kỳ (snapshot theo tháng)
htk as (
  select
    'HTK' as Bang,
    p.start_of_month as Ngay,
    cast(m.Nguon as string) as Nguon,
    cast(m.Ma_Kho as string) as Ma_Kho,
    safe_cast(m.ID_Hang as int64) as ID_Hang,
    safe_cast(m.So_Luong as bignumeric) as Sl_Ton,
    safe_cast(m.T_Tien1 as bignumeric) as T_Tien1,
    date(m.Ngay_Nhap) as Ngay_Nhap
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_hang_ton_kho` m
  cross join params p
  where safe_cast(m.Nam as int64) = extract(year from p.run_date)
    and safe_cast(m.Mm as int64) = extract(month from p.run_date)
),

-- 2.2 NX: nhập/xuất trong tháng (dấu theo Ma_Ct)
-- Logic: Nhập (NK, NM...) là dương, Xuất là âm
nx as (
  select
    'NX' as Bang,
    date(f.Ngay) as Ngay,
    cast(f.Nguon as string) as Nguon,
    cast(f.Ma_Kho as string) as Ma_Kho,
    safe_cast(f.ID_Hang as int64) as ID_Hang,

    (
      case
        when dm.Ma_Ct in ('NK','NM','NL','NS','PN') then 1
        else -1
      end
      * coalesce(safe_cast(f.So_Luong_Theo_Dvt as bignumeric), 0)
    ) as Sl_Ton,

    (
      case
        when dm.Ma_Ct in ('NK','NM','NL','NS','PN') then 1
        else -1
      end
      * coalesce(safe_cast(f.T_Tien1 as bignumeric), 0)
    ) as T_Tien1,

    case
      when dm.Ma_Ct in ('NK','NM','NL','NS','PN') then date(f.Ngay)
      else cast(null as date)
    end as Ngay_Nhap

  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` f
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan` dm
    on safe_cast(f.ID_Nx as int64) = safe_cast(dm.ID as int64)
    and cast(f.Nguon as string) = cast(dm.Nguon as string)
  cross join params p
  where date(f.Ngay) >= p.start_of_month
    and date(f.Ngay) <= p.run_date
),

-- 2.3 DT: doanh thu trong tháng (trừ kho)
dt_txn as (
  select
    'DT' as Bang,
    date(dt.Ngay_PhieuThu) as Ngay,
    cast(dt.Ma_Cong_Ty as string) as Nguon,
    cast(dt.Ma_Kho as string) as Ma_Kho,
    safe_cast(dt.ID_Hang as int64) as ID_Hang,
    (-coalesce(safe_cast(dt.So_Luong as bignumeric), 0)) as Sl_Ton,
    (-coalesce(safe_cast(dt.Gia_Von as bignumeric), 0)) as T_Tien1,
    cast(null as date) as Ngay_Nhap
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt
  cross join params p
  where date(dt.Ngay_PhieuThu) >= p.start_of_month
    and date(dt.Ngay_PhieuThu) <= p.run_date
),

-- 2.4 DCX: điều chuyển xuất (trừ kho)
dcx as (
  select
    'DCX' as Bang,
    date(dc.Ngay) as Ngay,
    cast(dc.Nguon as string) as Nguon,
    cast(dc.Ma_KhoX as string) as Ma_Kho,
    safe_cast(dc.ID_Hang as int64) as ID_Hang,
    (-coalesce(safe_cast(dc.So_luong as bignumeric), 0)) as Sl_Ton,
    (-coalesce(safe_cast(dc.T_Tien1 as bignumeric), 0)) as T_Tien1,
    cast(null as date) as Ngay_Nhap
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dieu_chuyen` dc
  cross join params p
  where date(dc.Ngay) >= p.start_of_month
    and date(dc.Ngay) <= p.run_date
),

-- 2.5 DCN: điều chuyển nhập (cộng kho)
dcn as (
  select
    'DCN' as Bang,
    date(dc.Ngay) as Ngay,
    cast(dc.Nguon as string) as Nguon,
    cast(dc.Ma_KhoN as string) as Ma_Kho,
    safe_cast(dc.ID_Hang as int64) as ID_Hang,
    coalesce(safe_cast(dc.So_luong as bignumeric), 0) as Sl_Ton,
    coalesce(safe_cast(dc.T_Tien1 as bignumeric), 0) as T_Tien1,
    date(dc.Ngay) as Ngay_Nhap
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dieu_chuyen` dc
  cross join params p
  where date(dc.Ngay) >= p.start_of_month
    and date(dc.Ngay) <= p.run_date
),

-- Gộp tất cả giao dịch
union_txn as (
  select * from htk
  union all select * from nx
  union all select * from dt_txn
  union all select * from dcx
  union all select * from dcn
),

-- Lấy Last_Ngay_Nhap theo rule: ưu tiên NX > HTK, rồi Ngay_Nhap mới nhất
ngay_nhap as (
  select
    Nguon,
    Ma_Kho,
    ID_Hang,
    Ngay_Nhap as Last_Ngay_Nhap
  from (
    select
      u.*,
      row_number() over (
        partition by Nguon, Ma_Kho, ID_Hang
        order by
          case when Bang = 'NX' then 2 when Bang = 'HTK' then 1 else 0 end desc,
          Ngay_Nhap desc
      ) as rn
    from union_txn u
    where Bang in ('HTK', 'NX')
      and Ngay_Nhap is not null
  )
  where rn = 1
),

-- Tồn kho/giá trị tổng hợp theo tháng
sl_ht_agg as (
  select
    u.Nguon,
    u.Ma_Kho,
    u.ID_Hang,

    sum(coalesce(u.Sl_Ton, 0)) as Sl_Ton,

    sum(case when u.Bang = 'HTK' then coalesce(u.Sl_Ton, 0) else 0 end) as Sl_Ton_Dau_Ky,

    sum(coalesce(u.T_Tien1, 0)) as T_Tien1,

    (
      sum(case when u.Bang = 'HTK' then coalesce(u.Sl_Ton, 0) else 0 end)
      + (
        sum(case when u.Bang != 'HTK' then coalesce(u.Sl_Ton, 0) else 0 end)
        * (select days_factor from params)
      )
    ) as Sl_Ton_Trung_Binh

  from union_txn u
  where (
    cast(u.Ma_Kho as string) like '%CH%'
    or cast(u.Ma_Kho as string) like '%GH%'
    or cast(u.Ma_Kho as string) in ('KPP','KCU','TMDT','B2B','BN1','HD1')
  )
  group by 1, 2, 3
),

sl_ht_final as (
  select
    a.*,
    n.Last_Ngay_Nhap
  from sl_ht_agg a
  left join ngay_nhap n
    on a.Nguon = n.Nguon and a.Ma_Kho = n.Ma_Kho and a.ID_Hang = n.ID_Hang
),

-- 5.1 Doanh thu agg: để lấy sản lượng bán
-- (Giữ logic giống PySpark, nhưng mở rộng lọc để loại cả #DTT# nếu có)
dt_agg as (
  select
    cast(dt.Ma_Cong_Ty as string) as Nguon,
    cast(dt.Ma_Kho as string) as Ma_Kho,
    safe_cast(dt.ID_Hang as int64) as ID_Hang,
    sum(coalesce(safe_cast(dt.So_Luong as bignumeric), 0)) as San_luong_ban
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt
  cross join params p
  where date(dt.Ngay_PhieuThu) >= p.start_of_month
    and date(dt.Ngay_PhieuThu) <= p.run_date
    and coalesce(safe_cast(dt.So_Luong as bignumeric), 0) > 0
    and coalesce(safe_cast(dt.Thanh_tien_theo_DG_ban as bignumeric), 0) > 0
    and (
      dt.Dien_giai_Phieu_thu is null
      or trim(upper(cast(dt.Dien_giai_Phieu_thu as string))) not in ('DTT#', '#DTT#', 'ĐTT#', '#ĐTT#')
    )
  group by 1, 2, 3
),

-- 5.2 Đặt cọc: (PySpark chỉ có upper bound <= run_date+2)
dc_agg as (
  select
    cast(dc.Nguon as string) as Nguon,
    cast(dc.Ma_Kho as string) as Ma_Kho,
    safe_cast(dc.ID_Hang as int64) as ID_Hang,
    sum(coalesce(safe_cast(dc.So_Luong as bignumeric), 0)) as Sl_Dat_Coc
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dat_coc` dc
  cross join params p
  where date(dc.Ngay) <= date_add(p.run_date, interval 2 day)
  group by 1, 2, 3
),

vbmm as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmvbmm`
),

joined as (
  select
    m.Nguon,
    p.start_of_month as Thang,
    p.run_date as Ngay,

    m.Ma_Kho,
    kho.Ten_Kho,

    trim(cast(h.Ma_Hang as string)) as Ma_Hang,
    cast(h.Ten_Hang as string) as Ten_Hang,

    cast(h.Ten_HangN as string) as ID_DaiChi,
    cast(h.Ten_Dai_Chi as string) as Ten_Dai_Chi,
    cast(h.Dai_Chi as string) as Dai_Chi,

    -- Ma_NCC theo rule Spark
    (
      case
        when length(cast(dt_dim.Ma_Dt as string)) >= 9 then 'Khach_Ban'
        when dt_dim.Ma_Dt is null then 'Khong_XD'
        else cast(dt_dim.Ma_Dt as string)
      end
    ) as Ma_NCC,

    cast(nhomct.Ten as string) as Danh_muc_SP,
    cast(nh.Ma_Nhom as string) as Nhom_hang,

    cast(nmm.Ma_NM as string) as Ma_mau,
    cast(chungl.Ten as string) as Chung_loai,
    cast(gioit.Ten as string) as Gioi_tinh,
    cast(hamlkl.Ten as string) as Ham_luong_kim_loai,
    cast(maubmkl.Ten as string) as Mau_sac,

    m.Last_Ngay_Nhap as Ngay_nhap,

    m.Sl_Ton,
    m.Sl_Ton_Dau_Ky,
    m.Sl_Ton_Trung_Binh,
    m.T_Tien1,

    dt_fact.San_luong_ban,
    dc_fact.Sl_Dat_Coc,

    cc.cc as CC,

    coalesce(safe_cast(h.T_Luong as bignumeric), 0) as T_Luong,
    coalesce(safe_cast(h.The_Tich as bignumeric), 0) as The_Tich,
    coalesce(safe_cast(h.Tien_Lai as bignumeric), 0) as Tien_Lai,
    coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0) as Tyle_Lai,

    coalesce(safe_cast(h.Gia_Ban_TT as bignumeric), 0) as Gia_Ban_TT,

    current_timestamp() as UpdateTime

  from sl_ht_final m
  cross join params p

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_kho` kho
    on cast(m.Ma_Kho as string) = cast(kho.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h
    on safe_cast(m.ID_Hang as int64) = safe_cast(h.ID as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(h.ID_Nhom as int64) = safe_cast(nh.ID as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom_ma_mau` nmm
    on safe_cast(h.ID_NMM as int64) = safe_cast(nmm.ID as int64)

  left join vbmm chungl
    on safe_cast(nmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
    and cast(chungl.cDM as string) = 'CHUNGL'

  left join vbmm gioit
    on safe_cast(nmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
    and cast(gioit.cDM as string) = 'GIOIT'

  left join vbmm hamlkl
    on safe_cast(nmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)
    and cast(hamlkl.cDM as string) = 'HAMLKL'

  left join vbmm maubmkl
    on safe_cast(nmm.ID_MauBMKL as int64) = safe_cast(maubmkl.ID_Stt as int64)
    and cast(maubmkl.cDM as string) = 'MAUBMKL'

  left join vbmm nhomct
    on safe_cast(nmm.ID_NhomCt as int64) = safe_cast(nhomct.ID_Stt as int64)
    and cast(nhomct.cDM as string) = 'NHOMCT'

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_doi_tac_1` dt_dim
    on safe_cast(h.ID_DtH as int64) = safe_cast(dt_dim.ID as int64)
    and cast(m.Nguon as string) = cast(dt_dim.Nguon as string)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_co_cau` cc
    on cast(h.Ten_HangN as string) = cast(cc.id_dai_chi as string)
    and cast(m.Ma_Kho as string) = cast(cc.ma_ch as string)

  left join dt_agg dt_fact
    on m.Nguon = dt_fact.Nguon and m.Ma_Kho = dt_fact.Ma_Kho and m.ID_Hang = dt_fact.ID_Hang

  left join dc_agg dc_fact
    on m.Nguon = dc_fact.Nguon and m.Ma_Kho = dc_fact.Ma_Kho and m.ID_Hang = dc_fact.ID_Hang
)

select
  Nguon,
  Thang,
  Ngay,
  Ma_Kho,
  Ten_Kho,
  Ma_Hang,
  Ten_Hang,
  ID_DaiChi,
  Ten_Dai_Chi,
  Dai_Chi,
  Ma_NCC,
  Danh_muc_SP,
  Nhom_hang,
  Ma_mau,
  Chung_loai,
  Gioi_tinh,
  Ham_luong_kim_loai,
  Mau_sac,
  Ngay_nhap,

  -- Metrics
  sum(coalesce(Sl_Ton, 0)) as Sl_Ton,
  sum(coalesce(Sl_Ton_Dau_Ky, 0)) as Sl_Ton_Dau_Ky,
  sum(coalesce(Sl_Ton_Trung_Binh, 0)) as Sl_Ton_Trung_Binh,

  sum(
    coalesce(Sl_Ton, 0)
    * (
      coalesce(T_Luong, 0)
      + coalesce(The_Tich, 0)
      + coalesce(Tien_Lai, 0)
      + coalesce(Tyle_Lai, 0)
    )
  ) as Tong_tlg,

  sum(coalesce(Sl_Ton, 0) * coalesce(T_Luong, 0)) as TL_Vang,
  sum(coalesce(San_luong_ban, 0) * coalesce(T_Luong, 0)) as TL_Vang_ban_ra,

  sum(
    case
      when substr(Ma_Hang, 1, 3) in ('KGB', 'KHS')
        or substr(Ma_Hang, 1, 4) in ('TTSJ', 'TTVR')
        or substr(Ma_Hang, 1, 2) in ('VD', '24', 'VT')
        then coalesce(Sl_Ton_Dau_Ky, 0) * coalesce(T_Luong, 0)
      else coalesce(Sl_Ton_Dau_Ky, 0)
        * (
          coalesce(T_Luong, 0)
          + coalesce(The_Tich, 0)
          + coalesce(Tien_Lai, 0)
          + coalesce(Tyle_Lai, 0)
        )
    end
  ) as Tong_tlg_Dau_Ky,

  sum(
    case
      when substr(Ma_Hang, 1, 3) in ('KGB', 'KHS')
        or substr(Ma_Hang, 1, 4) in ('TTSJ', 'TTVR')
        or substr(Ma_Hang, 1, 2) in ('VD', '24', 'VT')
        then coalesce(Sl_Ton_Trung_Binh, 0) * coalesce(T_Luong, 0)
      else coalesce(Sl_Ton_Trung_Binh, 0)
        * (
          coalesce(T_Luong, 0)
          + coalesce(The_Tich, 0)
          + coalesce(Tien_Lai, 0)
          + coalesce(Tyle_Lai, 0)
        )
    end
  ) as Tong_tlg_Trung_Binh,

  max(coalesce(safe_cast(CC as int64), 0)) as Sl_CC,

  sum(coalesce(San_luong_ban, 0)) as San_luong_ban,
  sum(coalesce(Sl_Dat_Coc, 0)) as Sl_Dat_Coc,

  -- Lưu ý: giữ đúng công thức trong PySpark (mặc dù có thể không trực giác)
  cast(sum(coalesce(T_Tien1, 0) * coalesce(Sl_Ton, 0)) as bignumeric) as Gia_Tri_Ton,

  cast(
    sum(
      case when coalesce(Sl_Ton_Dau_Ky, 0) != 0 then coalesce(T_Tien1, 0) * coalesce(Sl_Ton_Dau_Ky, 0) else 0 end
    ) as bignumeric
  ) as Gia_Tri_Ton_Dau_Ky,

  cast(
    sum(
      case when coalesce(Sl_Ton_Trung_Binh, 0) != 0 then coalesce(T_Tien1, 0) * coalesce(Sl_Ton_Trung_Binh, 0) else 0 end
    ) as bignumeric
  ) as Gia_Tri_Ton_Trung_Binh,

  sum(coalesce(T_Tien1, 0)) as Gia_nhap,
  sum(coalesce(Gia_Ban_TT, 0)) as Gia_ban_TT,

  max(UpdateTime) as UpdateTime

from joined

group by
  Nguon, Thang, Ngay,
  Ma_Kho, Ten_Kho,
  Ma_Hang, Ten_Hang,
  ID_DaiChi, Ten_Dai_Chi, Dai_Chi,
  Ma_NCC,
  Danh_muc_SP,
  Nhom_hang,
  Ma_mau,
  Chung_loai,
  Gioi_tinh,
  Ham_luong_kim_loai,
  Mau_sac,
  Ngay_nhap

