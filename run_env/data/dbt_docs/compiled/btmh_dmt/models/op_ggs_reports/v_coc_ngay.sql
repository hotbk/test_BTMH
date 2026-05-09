

with d_date as(
  SELECT 
    date,
    company
  FROM UNNEST(
      GENERATE_DATE_ARRAY(
          DATE '2026-01-01',
          LAST_DAY(DATE_ADD(current_date(), INTERVAL 1 QUARTER), QUARTER)
      )
  ) AS date
  CROSS JOIN UNNEST(['NY','SG','BN']) AS company
)

,khach_dat as(
  select Ngay as Ngay
        ,Nguon as Nguon
        ,sum(So_Luong) as so_luong_khach_dat
        ,sum(So_Luong * coalesce(h.T_Luong,0)) as t_luong_khach_dat
        ,sum(Tong_Tien) as tien_dat
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dat_coc` dc left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on dc.ID_Hang = h.ID
  where 1=1
  and ID_Dv >= 0
  and Ngay >= DATE '2026-01-01'
  and not (Nguon = 'HD' AND Ngay >= '2026-01-01')
  and Nganh_hang = 'Tích trữ'
  -- and h.Ma_Nhom in ('TT-24KD0', 'KGB', 'KHS', 'KTD', 'TTMVV24KD0', 'TTNTV24KD0', 'TTSJV24KD0', 'TTVRV24KD0', 'TTXVV24KD0')
  group by 1,2
)

,tra_coc as(
  select Ngay_PhieuThu
        ,Ma_Cong_Ty
        ,sum(case when (dt.Sub_ID is null or Sub_ID = '0') then So_Luong end) as so_luong_ban_ngay
        ,sum(case when (dt.Sub_ID is null or Sub_ID = '0') then SL_Chi_TT end) as t_luong_ban_ngay
        ,sum(case when (dt.Sub_ID is null or Sub_ID = '0') then Tien_PhieuThu1 end) as tien_ban_ngay
        ,sum(case when (dt.Sub_ID is not null and Sub_ID <> '0') then So_Luong end) as so_luong_tra_coc
        ,sum(case when (dt.Sub_ID is not null and Sub_ID <> '0') then SL_Chi_TT end) as t_luong_tra_coc
        ,sum(case when (dt.Sub_ID is not null and Sub_ID <> '0') then Tien_PhieuThu1 end) as tien_tra_coc
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on dt.ID_Hang = h.ID
  where 1=1
  and dt.ID_Dv >= 0
  and Ngay_PhieuThu >= DATE '2026-01-01'
  and not (Ma_Cong_Ty = 'HD' AND Ngay_PhieuThu >= '2026-01-01')
  and Nganh_hang = 'Tích trữ'
  -- and h.Ma_Nhom in ('TT-24KD0', 'KGB', 'KHS', 'KTD', 'TTMVV24KD0', 'TTNTV24KD0', 'TTSJV24KD0', 'TTVRV24KD0', 'TTXVV24KD0')
  group by 1,2
)


select d.date, d.company, tc.t_luong_ban_ngay, kd.t_luong_khach_dat, tc.t_luong_tra_coc, tc.so_luong_ban_ngay, kd.so_luong_khach_dat, tc.so_luong_tra_coc, kd.tien_dat, tc.tien_ban_ngay, tc.tien_tra_coc
from d_date d left join tra_coc tc on d.date = tc.Ngay_PhieuThu and d.company = tc.Ma_Cong_Ty
              left join khach_dat kd on d.date = kd.Ngay and d.company = kd.Nguon