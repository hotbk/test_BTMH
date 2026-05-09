with dat_coc AS (
  SELECT 
      concat(Ma_Kho,"-",Nguon) as Unique_Store_ID,
      concat(Nguon,"-",dc.ID) as Deposit_ID,
      Nguon,
      Ma_Kho,
      ID_hang,
      Ma_Hang as SKU_ID,
      Ngay_Giao AS Due_Date,
      Ma_Dt as Customer_ID,
      SUM(So_Luong) AS Required_Qty
  FROM `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dat_coc` dc
  LEFT JOIN `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h 
    ON dc.ID_Hang = h.ID
  WHERE ID_Dv >= 0
    -- AND Ngay >= '2025-01-01'
    -- AND Ngay_Giao >= current_date()
    AND Nguon IN ('NY', 'BN', 'SG')
    AND Ma_Nhom IN ('KGB', 'KHS')
    AND ID_Phieu_Thu IS NULL
  GROUP BY 1,2,3,4,5,6,7,8
)

select *
from dat_coc