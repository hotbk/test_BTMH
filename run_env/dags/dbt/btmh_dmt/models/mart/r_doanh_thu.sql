{% set full_snapshot = var('r_doanh_thu_full_snapshot', false) %}

{{
	config(
		materialized=('table' if full_snapshot else 'incremental'),
		incremental_strategy='insert_overwrite',
		on_schema_change='sync_all_columns',
		partition_by={"field": "Ngay", "data_type": "datetime", "granularity": "month"},
		cluster_by=['Ngay', 'ID']
	)
}}

{% set src_schema = (var('d_hang_schema', 'stg_augges_225') | lower) %}

with params as (
	select
		{{ btmh_run_date() }} as run_date,
		{{ btmh_start_date(4) }} as start_date
),

f_new as (
	select
		safe_cast(ID_Dv as int64) as ID_Dv,
		cast(ID as string) as ID,
		cast(Ngay_PhieuThu as datetime) as Ngay_PhieuThu,
		cast(Ma_Cong_Ty as string) as Ma_Cong_Ty,
		cast(ID_Phieu_thu as string) as ID_Phieu_thu,
		cast(STT_TrenPhieu as string) as STT_TrenPhieu,
		cast(Sub_ID as string) as Sub_ID,
		cast(ID_Hang as string) as ID_Hang,
		cast(Ma_Kho as string) as Ma_Kho,
		cast(Quay as string) as Quay,
		cast(Ma_Nv as string) as Ma_Nv,
		cast(Ma_KH as string) as Ma_KH,
		cast(Ma_Dt as string) as Ma_Dt,
		safe_cast(So_Luong as bignumeric) as So_Luong,
		safe_cast(SL_Chi_TT as bignumeric) as SL_Chi_TT,
		safe_cast(Don_gia_ban as bignumeric) as Don_gia_ban,
		safe_cast(Thanh_tien_theo_DG_ban as bignumeric) as Thanh_tien_theo_DG_ban,
		safe_cast(CK_phan_bo as bignumeric) as CK_phan_bo,
		safe_cast(CK_the_phan_bo as bignumeric) as CK_the_phan_bo,
		safe_cast(Gia_Von as bignumeric) as Gia_Von,
		safe_cast(CK_TheCn_phan_bo as bignumeric) as CK_TheCn_phan_bo,
		safe_cast(CK_TheMg_phan_bo as bignumeric) as CK_TheMg_phan_bo,
		safe_cast(Tien_PhieuThu1 as bignumeric) as Tien_PhieuThu1,
		safe_cast(Tien_Chiet_Khau as bignumeric) as Tien_Chiet_Khau,
		cast(InsertDate as timestamp) as InsertDate
	from {{ ref('f_doanh_thu') }}
	cross join params p
	where 1=1
	and Ma_dong is null
	{% if is_incremental() %}
		and date(Ngay_PhieuThu) >= date(p.start_date)
		and date(Ngay_PhieuThu) < date_add(date(p.run_date), interval 1 day)
	{% endif %}
),

h as (select * from {{ ref('d_hang') }}),
nh as (select * from {{ ref('d_nhom') }}),
nmm as (select * from {{ ref('d_nhom_ma_mau') }}),
kho as (select * from {{ ref('d_kho') }}),
dt as (select * from {{ ref('d_doi_tac') }}),
nv as (select * from {{ ref('d_nhan_vien') }}),
dvt as (select * from {{ ref('d_dvt') }}),

vbmm_chungl as (
	select ID_Stt, Ten
	from {{ source(src_schema, 'dmvbmm') }}
	where cDM = 'CHUNGL'
),
vbmm_gioit as (
	select ID_Stt, Ten
	from {{ source(src_schema, 'dmvbmm') }}
	where cDM = 'GIOIT'
),
vbmm_hamlkl as (
	select ID_Stt, Ten
	from {{ source(src_schema, 'dmvbmm') }}
	where cDM = 'HAMLKL'
),
vbmm_maubmkl as (
	select ID_Stt, Ten
	from {{ source(src_schema, 'dmvbmm') }}
	where cDM = 'MAUBMKL'
),
vbmm_nhomct as (
	select ID_Stt, Ten
	from {{ source(src_schema, 'dmvbmm') }}
	where cDM = 'NHOMCT'
),

new_rows as (
	select
		cast(f.ID as string) as ID,
		cast(f.Ngay_PhieuThu as datetime) as Ngay,
		cast(f.Ma_Cong_Ty as string) as `MA CONG TY`,
		cast(safe_cast(f.ID_Phieu_thu as int64) as string) as `ID PT`,
		cast(safe_cast(f.STT_TrenPhieu as int64) as string) as `STT PT`,
		cast(h.Ma_Hang as string) as `MA HANG`,
		cast(nmm.Ma_NM as string) as `MA MAU`,
		cast(h.Ten_HangN as string) as `ID DAI CHI`,
		cast(h.Dai_Chi as string) as `DAI CHI`,
		(
			case
				when substr(cast(h.Ma_Hang as string), 1, 2) in ('BA', 'BX', 'BC') then 'Bac'
				when substr(cast(h.Ma_Hang as string), 1, 2) in ('VT') then 'VT_DaMau'

				when cast(h.Ma_Hang as string) like 'PY%V18%' then 'PCY18K'
				when cast(h.Ma_Hang as string) like 'PY%V10%' then 'PCY10K'
				when cast(h.Ma_Hang as string) like 'CN%V18%' then 'PCY18K'
				when cast(h.Ma_Hang as string) like 'CN%V10%' then 'PCY10K'

				when substr(cast(h.Ma_Hang as string), 1, 2) = 'NC'
					and cast(h.Ma_Hang as string) not like 'NCCK%' then 'NhanCuoi'
				when substr(cast(h.Ma_Hang as string), 1, 2) = 'NC'
					and cast(h.Ma_Hang as string) like 'NCCK%' then 'NhanCuoiKC'

				when substr(cast(h.Ma_Hang as string), 1, 2) = 'PH' then 'PCH'
				when substr(cast(h.Ma_Hang as string), 1, 2) = 'NH' then 'TSNhapKhau'
				when substr(cast(h.Ma_Hang as string), 1, 2) = 'TK' then 'TSKimCuong'

				when substr(cast(h.Ma_Hang as string), 1, 2) = 'KC'
					or cast(h.Ma_Hang as string) like 'KD%KCN' then 'KimCuongVien'

				when substr(cast(h.Ma_Hang as string), 1, 2) = '24'
					and cast(h.Ma_Nhom_Lon as string) = 'PT' then 'PT'
				when substr(cast(h.Ma_Hang as string), 1, 2) = '24'
					and cast(h.Ma_Nhom_Lon as string) in ('2D', 'CN') then 'CN'
				when substr(cast(h.Ma_Hang as string), 1, 2) = '24'
					and cast(h.Ma_Nhom_Lon as string) = '3D' then '3D'
				when substr(cast(h.Ma_Hang as string), 1, 2) = '24'
					and cast(h.Ma_Nhom_Lon as string) = '5G' then 'CNC'

				when substr(cast(h.Ma_Hang as string), 1, 2) in ('VD') then 'VD'
				when substr(cast(h.Ma_Hang as string), 1, 4) in ('QTTV', 'QTDV', 'QTLG') then '24K_QT'
				when substr(cast(h.Ma_Hang as string), 1, 2) in ('NL') then 'NL'
				when substr(cast(h.Ma_Hang as string), 1, 3) in ('KGB', 'KHS') then 'KGB'
				when substr(cast(h.Ma_Hang as string), 1, 4) = 'TTSJ' then 'SJC'
				when substr(cast(h.Ma_Hang as string), 1, 4) = 'TTVR' then 'VRTL'

				when substr(cast(h.Ma_Hang as string), 1, 2) = 'PT' then 'PhongThuy'
				when cast(h.Ma_Hang as string) not like 'KD%KCN' then 'PhongThuy'
				when substr(cast(h.Ma_Hang as string), 1, 2) = 'QT'
					and substr(cast(h.Ma_Hang as string), 1, 4) not in ('QTTV', 'QTDV', 'QTLG') then 'QT'
			end
		) as `NHOM SAN PHAM`,
		cast(h.dong_san_pham as string) as `DONG SAN PHAM`,
		cast(h.Ma_Nhom as string) as `NHOM HANG`,
		cast(h.Ma_Nhom_Lon as string) as `MA NHOM LON`,
		cast(vbmm_nhomct.Ten as string) as `DANH MUC SAN PHAM`,
		cast(vbmm_chungl.Ten as string) as CHUNG_LOAI,
		cast(vbmm_gioit.Ten as string) as `GIOI TINH MA MAU`,
		cast(vbmm_hamlkl.Ten as string) as `HAM LUONG KIM LOAI`,
		cast(vbmm_maubmkl.Ten as string) as `MAU SAC`,
		cast(h.Ten_Hang as string) as `TEN HANG`,
		cast(h.Ten_In_hoa_don as string) as `TEN IN HOA DON`,
		cast(dvt.Ten_Dvt as string) as DVT,
		(
			case
				when cast(nh.Ma_Nhom as string) in ('NL24', 'NLTT', 'BTMVV24')
					or substr(cast(nh.Ma_Nhom as string), 1, 3) in ('KGB', 'KHS', 'TTS', 'TTV')
					then safe_cast(f.SL_Chi_TT as bignumeric)
				else safe_cast(f.So_Luong as bignumeric)
			end
		) as `SO LUONG`,
		safe_cast(f.SL_Chi_TT as bignumeric) as `SO LUONG CHI TT`,
		safe_cast(f.So_Luong as bignumeric) as `SO LUONG THEO DVT`,
		(
			coalesce(safe_cast(h.T_Luong as bignumeric), 0)
			* coalesce(safe_cast(f.So_Luong as bignumeric), 0)
		) as `TRONG LUONG VANG`,
		(
			(coalesce(safe_cast(h.The_Tich as bignumeric), 0) + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0))
			* coalesce(safe_cast(f.So_Luong as bignumeric), 0)
		) as `TRONG LUONG DA`,
		(
			coalesce(safe_cast(f.So_Luong as bignumeric), 0)
			* (
				coalesce(safe_cast(h.T_Luong as bignumeric), 0)
				+ coalesce(safe_cast(h.The_Tich as bignumeric), 0)
				+ coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
				+ coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
			)
		) as `TONG TRONG LUONG`,
		safe_cast(f.Don_gia_ban as bignumeric) as `DON GIA`,
		safe_cast(f.Thanh_tien_theo_DG_ban as bignumeric) as `THANH TIEN`,
		safe_cast(f.CK_phan_bo as bignumeric) as `TIEN CK PHAN BO`,
		safe_cast(f.CK_the_phan_bo as bignumeric) as `TIEN CK THE PHAN BO`,
		safe_cast(f.Gia_Von as bignumeric) as `TONG TIEN VON`,
		safe_cast(f.CK_TheCn_phan_bo as bignumeric) as `TIEN CK THE CN PHAN BO`,
		safe_cast(f.CK_TheMg_phan_bo as bignumeric) as `TIEN CK THE MG PHAN BO`,
		safe_cast(f.Tien_PhieuThu1 as bignumeric) as `DOANH THU THUAN`,
		safe_cast(f.Tien_Chiet_Khau as bignumeric) as `TIEN CHIET KHAU`,
		safe_cast(h.Tien_cong_ban as bignumeric) as `TIEN CONG BAN`,
		cast(f.Ma_Kho as string) as `MA CH`,
		cast(kho.Ten_Kho as string) as `TEN CH`,
		cast(dt.Dia_Chi as string) as `DIA CHI`,
		cast(null as float64) as Latitude,
		cast(null as float64) as Longtitude,
		cast(f.Quay as string) as `MA QUAY`,
		cast(f.Ma_Nv as string) as `ID NV QUAY`,
		cast(nv.Ten_Nv as string) as `TEN NV QUAY`,
		cast(f.Ma_KH as string) as `MA KH`,
		cast(f.Ma_Dt as string) as `Ma KH DT`,
		cast(dt.Ten_Dt as string) as `TEN KH`,
		cast(dt.Dien_Thoai as string) as `DIEN THOAI`,
		cast(dt.Gioi_Tinh as string) as `GIOI TINH`,
		cast(date(dt.Ngay_Sinh) as date) as `NGAY SINH`,
		(
			case
				when dt.Ngay_Sinh is null then ''
				else cast(date_diff(date(f.Ngay_PhieuThu), date(dt.Ngay_Sinh), year) as string)
			end
		) as TUOI,
		cast(dt.Tinh as string) as Tinh,
		cast(dt.Quan as string) as Quan,
		cast(f.InsertDate as timestamp) as InsertDate,
		cast(f.ID_Dv as int64) as ID_Dv,
		(
			case
				when f.Sub_ID is not null and cast(f.Sub_ID as string) != '0' then 'Phiếu bán trả cọc'
				else 'Phiếu trả ngay'
			end
		) as `LOAI PHIEU`,
		(
			case
				when f.Sub_ID is not null and cast(f.Sub_ID as string) != '0' then cast(f.Sub_ID as string)
				else cast(null as string)
			end
		) as `MA PHIEU COC`
	from f_new as f
	left join h on safe_cast(h.ID as int64) = safe_cast(f.ID_Hang as int64)
	left join nh on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)
	left join nmm on safe_cast(nmm.ID as int64) = safe_cast(h.ID_NMM as int64)
	left join kho on cast(kho.Ma_Kho as string) = cast(f.Ma_Kho as string)
	left join dt on cast(dt.Ma_Dt as string) = cast(f.Ma_Dt as string)
	left join nv on cast(nv.Ma_Nv as string) = cast(f.Ma_Nv as string)
	left join dvt on safe_cast(dvt.ID as int64) = safe_cast(h.ID_Dvt as int64)
	left join vbmm_nhomct on safe_cast(vbmm_nhomct.ID_Stt as int64) = safe_cast(nmm.ID_NhomCt as int64)
	left join vbmm_chungl on safe_cast(vbmm_chungl.ID_Stt as int64) = safe_cast(nmm.ID_ChungL as int64)
	left join vbmm_gioit on safe_cast(vbmm_gioit.ID_Stt as int64) = safe_cast(nmm.ID_GioiT as int64)
	left join vbmm_hamlkl on safe_cast(vbmm_hamlkl.ID_Stt as int64) = safe_cast(nmm.ID_HamLKL as int64)
	left join vbmm_maubmkl on safe_cast(vbmm_maubmkl.ID_Stt as int64) = safe_cast(nmm.ID_MauBMKL as int64)
),

old_rows as (
	{% if is_incremental() %}
		select * from new_rows where 1 = 0
	{% else %}
		select
			concat(
				cast(safe_cast(xlsx.`Id Pt` as int64) as string),
				'-',
				cast(safe_cast(xlsx.`Stt Pt` as int64) as string),
				'CU'
			) as ID,
			cast(xlsx.Ngay as datetime) as Ngay,
			cast(xlsx.`Ma Cong Ty` as string) as `MA CONG TY`,
			cast(safe_cast(xlsx.`Id Pt` as int64) as string) as `ID PT`,
			cast(safe_cast(xlsx.`Stt Pt` as int64) as string) as `STT PT`,
			cast(xlsx.`Ma Hang` as string) as `MA HANG`,
			cast(nmm_old.Ma_NM as string) as `MA MAU`,
			cast(xlsx.`Id Dai Chi` as string) as `ID DAI CHI`,
			cast(xlsx.`dai chi` as string) as `DAI CHI`,
			cast(xlsx.`Nhom San Pham` as string) as `NHOM SAN PHAM`,
			cast(xlsx.`Dong San Pham` as string) as `DONG SAN PHAM`,
			cast(xlsx.`Nhom Hang` as string) as `NHOM HANG`,
			cast(xlsx.`MA NHOM LON` as string) as `MA NHOM LON`,
			cast(xlsx.`Danh muc san pham` as string) as `DANH MUC SAN PHAM`,
			cast(xlsx.`Chung loai` as string) as CHUNG_LOAI,
			cast(xlsx.`GIOI TINH MA MAU` as string) as `GIOI TINH MA MAU`,
			cast(xlsx.`HAM LUONG KIM LOAI` as string) as `HAM LUONG KIM LOAI`,
			cast(xlsx.`MAU SAC` as string) as `MAU SAC`,
			cast(xlsx.`Ten Hang` as string) as `TEN HANG`,
			cast(xlsx.`Ten in hoa don` as string) as `TEN IN HOA DON`,
			cast(xlsx.`DVT` as string) as DVT,
			safe_cast(xlsx.`So Luong` as bignumeric) as `SO LUONG`,
			safe_cast(xlsx.`So Luong Chi Tt` as bignumeric) as `SO LUONG CHI TT`,
			safe_cast(xlsx.`SO LUONG _sp_` as bignumeric) as `SO LUONG THEO DVT`,
			safe_cast(xlsx.`TRONG LUONG VANG_BAC` as bignumeric) as `TRONG LUONG VANG`,
			safe_cast(xlsx.`TRONG LUONG DA` as bignumeric) as `TRONG LUONG DA`,
			safe_cast(xlsx.`TONG TRONG LUONG` as bignumeric) as `TONG TRONG LUONG`,
			safe_cast(xlsx.`Don Gia` as bignumeric) as `DON GIA`,
			safe_cast(xlsx.`Thanh Tien` as bignumeric) as `THANH TIEN`,
			cast(null as bignumeric) as `TIEN CK PHAN BO`,
			cast(null as bignumeric) as `TIEN CK THE PHAN BO`,
			safe_cast(xlsx.`Tong Tien Von` as bignumeric) as `TONG TIEN VON`,
			cast(null as bignumeric) as `TIEN CK THE CN PHAN BO`,
			cast(null as bignumeric) as `TIEN CK THE MG PHAN BO`,
			safe_cast(xlsx.`Doanh Thu Thuan` as bignumeric) as `DOANH THU THUAN`,
			safe_cast(xlsx.`Tien Chiet Khau` as bignumeric) as `TIEN CHIET KHAU`,
			safe_cast(xlsx.`TIEN CONG BAN` as bignumeric) as `TIEN CONG BAN`,
			cast(xlsx.`Cửa hàng` as string) as `MA CH`,
			cast(xlsx.`TEN CH` as string) as `TEN CH`,
			cast(dt_old.Dia_Chi as string) as `DIA CHI`,
			safe_cast(xlsx.`Latitude` as float64) as Latitude,
			safe_cast(xlsx.`Longtitude` as float64) as Longtitude,
			cast(xlsx.`Ma Quay` as string) as `MA QUAY`,
			cast(xlsx.`Id Nv Quay` as string) as `ID NV QUAY`,
			cast(xlsx.`Ten Nv Quay` as string) as `TEN NV QUAY`,
			cast(xlsx.`Ma Kh` as string) as `MA KH`,
			cast(xlsx.`Ma KH DT` as string) as `Ma KH DT`,
			cast(dt_old.Ten_Dt as string) as `TEN KH`,
			cast(xlsx.`DIEN THOAI` as string) as `DIEN THOAI`,
			cast(dt_old.Gioi_Tinh as string) as `GIOI TINH`,
			cast(date(dt_old.Ngay_Sinh) as date) as `NGAY SINH`,
			(
				case
					when dt_old.Ngay_Sinh is null then ''
					else cast(date_diff(cast(date(xlsx.Ngay) as date), date(dt_old.Ngay_Sinh), year) as string)
				end
			) as TUOI,
			cast(dt_old.Tinh as string) as Tinh,
			cast(dt_old.Quan as string) as Quan,
			cast(null as timestamp) as InsertDate,
			cast(0 as int64) as ID_Dv,
			cast('Phiếu trả ngay' as string) as `LOAI PHIEU`,
			cast(null as string) as `MA PHIEU COC`
		from {{ source('ggs', 'xlsx_doanh_thu_cu') }} as xlsx
		left join {{ ref('d_hang') }} as h_map
			on cast(xlsx.`Ma Hang` as string) = cast(h_map.Ma_Hang as string)
		left join nmm as nmm_old
			on safe_cast(nmm_old.ID as int64) = safe_cast(h_map.ID_NMM as int64)
		left join dt as dt_old
			on cast(dt_old.Ma_Dt as string) = cast(xlsx.`Ma KH DT` as string)
		where cast(xlsx.`Ma Cong Ty` as string) = 'CU'
			and cast(xlsx.Ngay as date) < date('2024-01-01')
	{% endif %}
)

select * from new_rows
union all
select * from old_rows
