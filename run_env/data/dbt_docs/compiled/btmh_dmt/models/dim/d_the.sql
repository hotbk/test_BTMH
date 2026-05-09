with src as (
    
    select
        'NY' as Nguon,
        ID,
        ID_Dv,
        So_The,
        Ten_The as Ten_The,
        ID_Nhom,
        ID_LThe,
        ID_Dt,
        Ngay_Cap,
        Tu_Ngay,
        Den_Ngay,
        cast(Con_Lai as numeric) as Con_Lai,
        cast(So_Luong as numeric) as So_Luong,
        Tu_Seri,
        Den_Seri,
        ID_KhThe,
        DaKh,
        cast(Diem_Dk as numeric) as Diem_Dk,
        cast(Diem_DkQd as numeric) as Diem_DkQd,
        IsTheEdit,
        ID_HD,
        Ghi_Chu,
        IsAuto,
        IsNewTN,
        IsEDitTN,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        SNgayT,
        SNgayD
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmthe`
    where upper(Ten_The) like '%VOU%'
    
    union all
    
    
    select
        'SX' as Nguon,
        ID,
        ID_Dv,
        So_The,
        Ten_The as Ten_The,
        ID_Nhom,
        ID_LThe,
        ID_Dt,
        Ngay_Cap,
        Tu_Ngay,
        Den_Ngay,
        cast(Con_Lai as numeric) as Con_Lai,
        cast(So_Luong as numeric) as So_Luong,
        Tu_Seri,
        Den_Seri,
        ID_KhThe,
        DaKh,
        cast(Diem_Dk as numeric) as Diem_Dk,
        cast(Diem_DkQd as numeric) as Diem_DkQd,
        IsTheEdit,
        ID_HD,
        Ghi_Chu,
        IsAuto,
        IsNewTN,
        IsEDitTN,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        SNgayT,
        SNgayD
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmthe`
    where upper(Ten_The) like '%VOU%'
    
    union all
    
    
    select
        'HD' as Nguon,
        ID,
        ID_Dv,
        So_The,
        Ten_The as Ten_The,
        ID_Nhom,
        ID_LThe,
        ID_Dt,
        Ngay_Cap,
        Tu_Ngay,
        Den_Ngay,
        cast(Con_Lai as numeric) as Con_Lai,
        cast(So_Luong as numeric) as So_Luong,
        Tu_Seri,
        Den_Seri,
        ID_KhThe,
        DaKh,
        cast(Diem_Dk as numeric) as Diem_Dk,
        cast(Diem_DkQd as numeric) as Diem_DkQd,
        IsTheEdit,
        ID_HD,
        Ghi_Chu,
        IsAuto,
        IsNewTN,
        IsEDitTN,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        SNgayT,
        SNgayD
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmthe`
    where upper(Ten_The) like '%VOU%'
    
    union all
    
    
    select
        'BN' as Nguon,
        ID,
        ID_Dv,
        So_The,
        Ten_The as Ten_The,
        ID_Nhom,
        ID_LThe,
        ID_Dt,
        Ngay_Cap,
        Tu_Ngay,
        Den_Ngay,
        cast(Con_Lai as numeric) as Con_Lai,
        cast(So_Luong as numeric) as So_Luong,
        Tu_Seri,
        Den_Seri,
        ID_KhThe,
        DaKh,
        cast(Diem_Dk as numeric) as Diem_Dk,
        cast(Diem_DkQd as numeric) as Diem_DkQd,
        IsTheEdit,
        ID_HD,
        Ghi_Chu,
        IsAuto,
        IsNewTN,
        IsEDitTN,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        SNgayT,
        SNgayD
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmthe`
    where upper(Ten_The) like '%VOU%'
    
    union all
    
    
    select
        'SG' as Nguon,
        ID,
        ID_Dv,
        So_The,
        Ten_The as Ten_The,
        ID_Nhom,
        ID_LThe,
        ID_Dt,
        Ngay_Cap,
        Tu_Ngay,
        Den_Ngay,
        cast(Con_Lai as numeric) as Con_Lai,
        cast(So_Luong as numeric) as So_Luong,
        Tu_Seri,
        Den_Seri,
        ID_KhThe,
        DaKh,
        cast(Diem_Dk as numeric) as Diem_Dk,
        cast(Diem_DkQd as numeric) as Diem_DkQd,
        IsTheEdit,
        ID_HD,
        Ghi_Chu,
        IsAuto,
        IsNewTN,
        IsEDitTN,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        SNgayT,
        SNgayD
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmthe`
    where upper(Ten_The) like '%VOU%'
    
    
)

select * except(_rn)
from (
    select
        src.*,
        row_number() over (
            partition by cast(src.Nguon as string), safe_cast(src.ID as int64)
            order by datetime(coalesce(src.LastEdit, src.InsertDate)) desc
        ) as _rn
    from src
)
where _rn = 1