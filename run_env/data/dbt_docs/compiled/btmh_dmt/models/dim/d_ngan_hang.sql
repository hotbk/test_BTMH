with src as (
	
	select
		'NY' as Nguon,
		ID_Dv,
		Ma_Nh,
		Ten_Nh as Ten_Nh,
		Ten_NhE,
		So_Tk,
		Noi_Mo,
		Dia_Chi,
		ID_Nhom,
		ID_Loai,
		Ghi_Chu,
		InsertDate,
		LastEdit,
		UserEdit,
		UserID,
		Inactive,
		Ky_Hieu,
		Thu_Huong as Thu_Huong,
		current_timestamp() as UpdateTime
	from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnhang`
	
	union all
	
	
	select
		'SX' as Nguon,
		ID_Dv,
		Ma_Nh,
		Ten_Nh as Ten_Nh,
		Ten_NhE,
		So_Tk,
		Noi_Mo,
		Dia_Chi,
		ID_Nhom,
		ID_Loai,
		Ghi_Chu,
		InsertDate,
		LastEdit,
		UserEdit,
		UserID,
		Inactive,
		Ky_Hieu,
		Thu_Huong as Thu_Huong,
		current_timestamp() as UpdateTime
	from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmnhang`
	
	union all
	
	
	select
		'HD' as Nguon,
		ID_Dv,
		Ma_Nh,
		Ten_Nh as Ten_Nh,
		Ten_NhE,
		So_Tk,
		Noi_Mo,
		Dia_Chi,
		ID_Nhom,
		ID_Loai,
		Ghi_Chu,
		InsertDate,
		LastEdit,
		UserEdit,
		UserID,
		Inactive,
		Ky_Hieu,
		Thu_Huong as Thu_Huong,
		current_timestamp() as UpdateTime
	from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmnhang`
	
	union all
	
	
	select
		'BN' as Nguon,
		ID_Dv,
		Ma_Nh,
		Ten_Nh as Ten_Nh,
		Ten_NhE,
		So_Tk,
		Noi_Mo,
		Dia_Chi,
		ID_Nhom,
		ID_Loai,
		Ghi_Chu,
		InsertDate,
		LastEdit,
		UserEdit,
		UserID,
		Inactive,
		Ky_Hieu,
		Thu_Huong as Thu_Huong,
		current_timestamp() as UpdateTime
	from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmnhang`
	
	union all
	
	
	select
		'SG' as Nguon,
		ID_Dv,
		Ma_Nh,
		Ten_Nh as Ten_Nh,
		Ten_NhE,
		So_Tk,
		Noi_Mo,
		Dia_Chi,
		ID_Nhom,
		ID_Loai,
		Ghi_Chu,
		InsertDate,
		LastEdit,
		UserEdit,
		UserID,
		Inactive,
		Ky_Hieu,
		Thu_Huong as Thu_Huong,
		current_timestamp() as UpdateTime
	from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmnhang`
	
	
)

select distinct * from src