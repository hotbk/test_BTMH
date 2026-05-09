with src as (
    
    select
        'NY' as Nguon,
        dt.ID,
        dt.Ma_Dt,
        dt.Ten_Dt as Ten_Dt,
        dt.Dia_chi as Dia_chi,
        dt.Dien_Thoai,
        dt.Gioi_Tinh as Gioi_Tinh,
        dt.Ngay_Sinh,
        dt.So_CMT,
        t.Ten_tinh as Tinh,
        q.Ten_Quan as Quan,
        p.Ten_Phuong as Phuong,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmdt` dt
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmtinh` t
        on dt.ID_Tinh = t.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmquan` q
        on dt.ID_Quan = q.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmphuong` p
        on dt.ID_Phuong = p.ID
    
    union all
    
    
    select
        'SX' as Nguon,
        dt.ID,
        dt.Ma_Dt,
        dt.Ten_Dt as Ten_Dt,
        dt.Dia_chi as Dia_chi,
        dt.Dien_Thoai,
        dt.Gioi_Tinh as Gioi_Tinh,
        dt.Ngay_Sinh,
        dt.So_CMT,
        t.Ten_tinh as Tinh,
        q.Ten_Quan as Quan,
        p.Ten_Phuong as Phuong,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmdt` dt
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmtinh` t
        on dt.ID_Tinh = t.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmquan` q
        on dt.ID_Quan = q.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmphuong` p
        on dt.ID_Phuong = p.ID
    
    union all
    
    
    select
        'HD' as Nguon,
        dt.ID,
        dt.Ma_Dt,
        dt.Ten_Dt as Ten_Dt,
        dt.Dia_chi as Dia_chi,
        dt.Dien_Thoai,
        dt.Gioi_Tinh as Gioi_Tinh,
        dt.Ngay_Sinh,
        dt.So_CMT,
        t.Ten_tinh as Tinh,
        q.Ten_Quan as Quan,
        p.Ten_Phuong as Phuong,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmdt` dt
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmtinh` t
        on dt.ID_Tinh = t.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmquan` q
        on dt.ID_Quan = q.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmphuong` p
        on dt.ID_Phuong = p.ID
    
    union all
    
    
    select
        'BN' as Nguon,
        dt.ID,
        dt.Ma_Dt,
        dt.Ten_Dt as Ten_Dt,
        dt.Dia_chi as Dia_chi,
        dt.Dien_Thoai,
        dt.Gioi_Tinh as Gioi_Tinh,
        dt.Ngay_Sinh,
        dt.So_CMT,
        t.Ten_tinh as Tinh,
        q.Ten_Quan as Quan,
        p.Ten_Phuong as Phuong,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmdt` dt
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmtinh` t
        on dt.ID_Tinh = t.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmquan` q
        on dt.ID_Quan = q.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmphuong` p
        on dt.ID_Phuong = p.ID
    
    union all
    
    
    select
        'SG' as Nguon,
        dt.ID,
        dt.Ma_Dt,
        dt.Ten_Dt as Ten_Dt,
        dt.Dia_chi as Dia_chi,
        dt.Dien_Thoai,
        dt.Gioi_Tinh as Gioi_Tinh,
        dt.Ngay_Sinh,
        dt.So_CMT,
        t.Ten_tinh as Tinh,
        q.Ten_Quan as Quan,
        p.Ten_Phuong as Phuong,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmdt` dt
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmtinh` t
        on dt.ID_Tinh = t.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmquan` q
        on dt.ID_Quan = q.ID
    left join `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmphuong` p
        on dt.ID_Phuong = p.ID
    
    
)

select * from src