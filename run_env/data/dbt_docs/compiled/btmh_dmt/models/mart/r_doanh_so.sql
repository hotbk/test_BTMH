

with doanh_thu as (
    select Ngay_PhieuThu
          ,cast(Ma_Cong_Ty as string) as Ma_Cong_Ty
          ,cast(ID_Phieu_thu as string) as ID_Phieu_thu
          ,InsertDate
          ,cast(Ma_Dt as string) as Ma_KH
          ,cast(ID_Hang as string) as ID_Hang
          ,cast(Sub_ID as string) as Sub_ID
          ,Ma_Kho
          ,sum(So_Luong) as So_Luong
          ,sum(SL_Chi_TT) as SL_Chi_TT
          ,sum(Gia_Von) as Gia_Von
          ,sum(Tien_PhieuThu1) as Tien_PhieuThu1
    from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt
    where 1=1
    and Ma_Cong_Ty in ('BN', 'NY', 'SX', 'SG', 'HD')
    and not (Ma_Cong_Ty = 'HD' AND Ngay_PhieuThu >= '2026-01-01')
    -- and Ngay_PhieuThu < cast(current_date() as date)
    and Ma_dong is null
    and ID_Dv >= 0
    group by Ngay_PhieuThu, Ma_Cong_Ty, ID_Phieu_thu, Ma_KH, Sub_ID, ID_Hang, Ma_Kho, InsertDate
)

,dat_coc as (
    select Nguon
          ,cast(ID as string) as ID
          ,cast(ID_Hang as string) as ID_Hang
          ,Thoi_diem_tao
          ,cast(Thoi_diem_tao as date) as Ngay
          ,case when Ngay_Giao < Ngay then Ngay else Ngay_Giao end as Ngay_Giao
          ,Ma_Kho
          ,cast(Ma_DT as string) as Ma_KH
          ,cast(ID_Phieu_Thu as string) as Sub_ID
          ,sum(So_Luong) as So_Luong
          ,sum(Tong_Tien) as Tong_Tien
    from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dat_coc` dc
    where 1=1
    and Nguon in ('BN', 'NY', 'SX', 'SG', 'HD')
    and not (Nguon = 'HD' AND Ngay >= '2026-01-01')
    and ID_Dv >= 0
    group by Nguon, ID, ID_Hang, Ngay_Giao, Ma_Kho, Ma_KH, Ma_DT, Sub_ID, Thoi_diem_tao
)

,coc_inf as (
    select distinct Nguon, ID, Ngay, Thoi_diem_tao, Ngay_Giao
    from dat_coc 
)

,cogs as (
    select Ngay_Ct as ngay, 
           sum(T_Tien1) / sum(So_Luong_Theo_Dvt) as Gia_Mua_Trung_Binh
    from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` nx
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnx` dnx 
        on nx.ID_Nx = dnx.ID
    left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h 
        on nx.ID_Hang = h.ID
    where dnx.Ma_Ct = 'NM'
      and h.Ma_Hang = 'NL9999KD'
      and Ngay_Ct < '2025-12-08'
    group by 1

    union all
    
    select ngay, Gia_Mua_Trung_Binh
    from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_ty_gia`
    where ngay >= '2025-12-08'
      and Ma_VBTG = 'NL9999'
)

,coc_chua_tra as (
    select concat(Nguon, '_', dc.ID, '_1') as order_id
          ,concat(Nguon, '_', dc.ID, '_', ID_Hang, '_1') as order_line_id
          ,ID_Hang as item_id 
          ,h.Ma_Hang as item_code
          ,h.Ten_Hang as item_name
          ,nh.Ma_Nhom as ma_nhom
          ,date(dc.Ngay) as created_date
          ,cast(Thoi_diem_tao as datetime) as created_time
          ,cast(Ngay_Giao as date) as expected_date
          ,cast(null as date) as bill_date
          ,cast(null as string) as bill_id
          ,Ma_KH as customer_id
          ,Nguon as company_code
          ,Ma_Kho as warehouse_id
          ,Ma_Kho as shop_code
          ,So_Luong as quantity_by_unit
          ,case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then So_Luong * coalesce(h.T_Luong,0) else So_Luong end as quantity
          ,Tong_Tien as line_income
          ,case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then So_Luong * coalesce(h.T_Luong,0)* gia_mua_trung_binh + 50000 * So_Luong  
                else So_Luong * gia_mua_trung_binh end as cogs
          ,(gia_mua_trung_binh + 50000) as cogs_per_unit
          ,case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then Tong_Tien - (So_Luong * coalesce(h.T_Luong,0)* gia_mua_trung_binh + 50000 * So_Luong) 
                else Tong_Tien - (So_Luong * gia_mua_trung_binh) end as est_gross_profit
          ,'Cọc chưa trả' as type
    from dat_coc dc left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h on dc.ID_Hang = cast(h.ID as string)
                    left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh on h.ID_Nhom = nh.ID
                    left join cogs on cast(dc.Ngay as date) = cogs.ngay
    where 1=1
    and Sub_ID is null
)

,coc_da_tra as (
    select concat(coalesce(Nguon, Ma_Cong_Ty), '_', coalesce(dc.ID,ID_Phieu_thu), '_2') as order_id
          ,concat(Nguon, '_', dc.ID, '_', ID_Hang, '_2') as order_line_id
          ,ID_Hang as item_id 
          ,h.Ma_Hang as item_code
          ,h.Ten_Hang as item_name
          ,nh.Ma_Nhom as ma_nhom
          ,coalesce(dc.Ngay, Ngay_PhieuThu) as created_date
          ,cast(coalesce(Thoi_diem_tao, InsertDate) as datetime) as created_time
          ,cast(Ngay_Giao as date) as expected_date
          ,Ngay_PhieuThu as bill_date
          ,ID_Phieu_thu as bill_id
          ,Ma_KH as customer_id
          ,Ma_Cong_Ty as company_code
          ,Ma_Kho as warehouse_id
          ,Ma_Kho as shop_code
          ,dt.So_Luong as quantity_by_unit
          ,SL_Chi_TT as quantity
          ,Tien_PhieuThu1 as line_income
          ,case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then (gia_mua_trung_binh * SL_Chi_TT + 50000 * dt.So_Luong) else Gia_Von end as cogs
          ,coalesce(Gia_Von / nullif(SL_Chi_TT,0),0) as cogs_per_unit
          ,Tien_PhieuThu1 - case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then (gia_mua_trung_binh * SL_Chi_TT + 50000 * dt.So_Luong) else Gia_Von end as gross_profit
          ,'Cọc đã trả' as type
    from doanh_thu dt left join coc_inf dc on dt.Sub_ID = dc.ID and dt.Ma_Cong_Ty = dc.Nguon
                      left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h on dt.ID_Hang = cast(h.ID as string)
                      left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh on h.ID_Nhom = nh.ID
                      left join cogs on coalesce(dc.Ngay, Ngay_PhieuThu) = cogs.ngay
    where 1=1
    and dt.Sub_ID is not null 
    and dt.Sub_ID <> '0'
    and SL_Chi_TT is not null 
    -- and nh.Ma_Nhom <> 'NLTT'
)

,don_tra_ngay as (
    select concat(Ma_Cong_Ty, '_', ID_Phieu_thu, '_3') as order_id
          ,concat(Ma_Cong_Ty, '_', ID_Phieu_thu, '_', ID_Hang, '_3') as order_line_id
          ,ID_Hang as item_id 
          ,h.Ma_Hang as item_code
          ,h.Ten_Hang as item_name
          ,nh.Ma_Nhom as ma_nhom
          ,Ngay_PhieuThu as created_date
          ,cast(InsertDate as datetime) as created_time
          ,cast(Ngay_PhieuThu as date) as expected_date
          ,Ngay_PhieuThu as bill_date
          ,ID_Phieu_thu as bill_id
          ,Ma_KH as customer_id
          ,Ma_Cong_Ty as company_code
          ,Ma_Kho as warehouse_id
          ,Ma_Kho as shop_code
          ,dt.So_Luong as quantity_by_unit
          ,SL_Chi_TT as quantity
          ,Tien_PhieuThu1 as line_income
          ,case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then (gia_mua_trung_binh * SL_Chi_TT + 50000 * dt.So_Luong) else Gia_Von end as cogs
          ,coalesce(Gia_Von / nullif(SL_Chi_TT,0),0) as cogs_per_unit
          ,Tien_PhieuThu1 - case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then (gia_mua_trung_binh * SL_Chi_TT + 50000 * dt.So_Luong) else Gia_Von end as gross_profit
          ,'Đơn trả ngay' as type
    from doanh_thu dt left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h on dt.ID_Hang = cast(h.ID as string)
                      left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh on h.ID_Nhom = nh.ID
                      left join cogs on dt.Ngay_PhieuThu = cogs.ngay
    where 1=1
    and (dt.Sub_ID is null or dt.Sub_ID = '0')
    and SL_Chi_TT is not null 
    
    -- and nh.Ma_Nhom is null
)

,first_merge as (
    select * from coc_chua_tra
    union all
    select * from don_tra_ngay
)

,don_b2c as (
    select
        order_id,
        order_line_id,
        item_id,
        item_code,
        item_name,
        ma_nhom,
        date(created_date) as created_date,
        created_time,
        expected_date,
        bill_date,
        bill_id,
        customer_id,
        company_code,
        warehouse_id,
        shop_code,
        quantity_by_unit,
        quantity,
        line_income,
        cogs as est_cogs,
        cogs_per_unit as cogs_per_unit,
        est_gross_profit as est_gross_profit,
        type
    from first_merge

    union all

    select *
    from coc_da_tra
)

,don_b2b as (
    select concat(Loai_Chung_Tu, '_', ID_Phieu_Ban, '_4') as order_id
          ,concat(Loai_Chung_Tu, '_', ID_Phieu_Ban, '_', ID_Hang, '_4') as order_line_id
          ,cast(ID_Hang as string) as item_id 
          ,h.Ma_Hang as item_code
          ,h.Ten_Hang as item_name
          ,nh.Ma_Nhom as ma_nhom
          ,Ngay_Chung_Tu as created_date
          ,cast(Ngay_Chung_Tu as datetime) as created_time
          ,cast(Ngay_Chung_Tu as date) as expected_date
          ,Ngay_Chung_Tu as bill_date
          ,ID_Phieu_Ban as bill_id
          ,Ma_Khach_Hang as customer_id
          ,Loai_Chung_Tu as company_code
          ,Ma_Kho as warehouse_id
          ,Ma_Kho as shop_code
          ,So_Luong as quantity_by_unit
          ,SL_Chi_TT as quantity
          ,Doanh_Thu_Ban as line_income
          ,case when left(nh.Ma_Nhom,3) in ('KGB', 'KHS', 'TTS', 'TTV') then (gia_mua_trung_binh * SL_Chi_TT + 50000 * dt.So_Luong) else Tong_Gia_Von end as cogs
          ,coalesce(Tong_Gia_Von / nullif(SL_Chi_TT,0),0) as cogs_per_unit
          ,Loi_Nhuan_Gop as est_gross_profit
          ,'Đơn B2B' as type
    from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_b2b` dt left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h on dt.ID_Hang = h.ID
                      left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh on h.ID_Nhom = nh.ID
                      left join cogs on dt.Ngay_Chung_Tu = cogs.ngay
    where 1=1
    and Ma_Kho = 'B2BBL'
)

,final as (
    select *
    from don_b2c
    union all
    select *
    from don_b2b
)

select
    order_id,
    order_line_id,
    item_id,
    item_code,
    item_name,
    ma_nhom,
    date(created_date) as created_date, 
    created_time,
    expected_date,
    bill_date,
    bill_id,
    customer_id,
    company_code,
    warehouse_id,
    shop_code,
    quantity_by_unit,
    quantity,
    line_income,
    est_cogs,
    cogs_per_unit,
    est_gross_profit,
    type
from final
where 1=1